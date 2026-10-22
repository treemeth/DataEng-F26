# backup.py
import json
import os
import threading
import time
import gzip
import shutil
from datetime import datetime, timedelta, timezone

import google.auth
from google.cloud import pubsub_v1

PROJECT_ID         = None     # auto-detected on a GCE VM; set only if needed
SUBSCRIPTION_ID    = 'backup_sub'
IDLE_TIMEOUT       = 60       # Quiet period before reporting; does not stop the program.
BACKUP_DIR = 'backups'
os.makedirs(BACKUP_DIR, exist_ok=True)

# At most this many messages are held unacknowledged at once.
flow_control = pubsub_v1.types.FlowControl(max_messages=1000)


def resolve_project_id(explicit=None):
    project = explicit or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project:
        _, project = google.auth.default()
    if not project:
        raise SystemExit('Could not determine the GCP project ID. Set PROJECT_ID '
                         'in this script or export GOOGLE_CLOUD_PROJECT.')
    return project


class RunTracker:
    """Tracks one run without retaining breadcrumb records."""

    def __init__(self, run_id):
        self.total = None
        self.seen = bytearray()
        self.unique = 0
        self.duplicates = 0
        self.max_seq = -1
        self.bad_records = 0

        self.vehicles = set()
        self.trips = set()
        self.earliest = None
        self.latest = None

        self.first_received = None
        self.last_received = None
        self.first_mono = None
        self.last_mono = None
        self.last_activity = time.monotonic()

        run_date = datetime.strptime(
            run_id.split('-')[0], '%Y%m%d'
        ).strftime('%Y-%m-%d')

        self.file_path = os.path.join(
            BACKUP_DIR, f'breadcrumbs_{run_date}_.log'
        )
        self.compressed_path = self.file_path + '.gz'
        self.uncompressed_bytes = 0
        self.compression_finished = None
        self.compression_finished_mono = None
        self.finalizing = False

    def add(self, seq):
        byte, bit = divmod(seq, 8)

        if byte >= len(self.seen):
            self.seen.extend(b'\x00' * (byte + 1 - len(self.seen)))

        mask = 1 << bit

        if self.seen[byte] & mask:
            self.duplicates += 1
            return False

        self.seen[byte] |= mask
        self.unique += 1
        self.max_seq = max(self.max_seq, seq)
        return True

    def contains(self, seq):
        byte, bit = divmod(seq, 8)
        return (
            byte < len(self.seen)
            and bool(self.seen[byte] & (1 << bit))
        )

    @property
    def missing(self):
        if self.total is None:
            return None
        return self.total - self.unique


subscriber = pubsub_v1.SubscriberClient()
sub_path   = subscriber.subscription_path(resolve_project_id(PROJECT_ID), SUBSCRIPTION_ID)

# Fail fast with a clear error if the subscription is missing or we lack permission.
subscriber.get_subscription(subscription=sub_path)

# Shared state: callbacks run on several worker threads.
lock = threading.Lock()
runs = {}
untracked = 0
bad_payloads = 0

# Keep only recent completed RunIDs, so late deliveries do not
# accidentally start a second report for the same run.
closed_runs = {}
CLOSED_RUN_LIMIT = 32


def callback(message):
    global untracked, bad_payloads

    try:
        payload = json.loads(message.data)
        if not isinstance(payload, dict):
            raise ValueError('Expected a JSON object')
    except ValueError:
        with lock:
            bad_payloads += 1
        message.ack()
        return

    attrs = message.attributes

    try:
        run_id = attrs['run_id']
        if not run_id:
            raise ValueError('Empty RunID')

        is_summary = attrs.get('message_type') == 'run_summary'

        if is_summary:
            total = payload['expected_total']
            if type(total) is not int or total < 0:
                raise ValueError('Invalid expected total')
        else:
            seq = int(attrs['seq'])
            if seq < 0:
                raise ValueError('Negative sequence number')

    except (KeyError, TypeError, ValueError):
        with lock:
            untracked += 1
        message.ack()
        return

    with lock:
        if run_id not in closed_runs:
            if run_id not in runs:
                runs[run_id] = RunTracker(run_id)

            tracker = runs[run_id]
            if tracker.finalizing:
                message.nack()
                return
            now_mono = time.monotonic()

            if is_summary:
                if (tracker.max_seq >= total or (tracker.total is not None and tracker.total != total)):
                    untracked += 1
                else:
                    tracker.total = total
                    tracker.last_activity = now_mono

            elif tracker.total is not None and seq >= tracker.total:
                untracked += 1

            else:
                now = datetime.now(timezone.utc)
                tracker.last_activity = now_mono

                if tracker.first_received is None:
                    tracker.first_received = now
                    tracker.first_mono = now_mono

                tracker.last_received = now
                tracker.last_mono = now_mono

                if not tracker.contains(seq):
                    # Append before marking the sequence received.
                    line = message.data + b'\n'

                    try:
                        with open(tracker.file_path, 'ab') as out:
                            original_size = out.tell()

                            try:
                                out.write(line)
                                out.flush()
                                os.fsync(out.fileno())
                            except OSError:
                                # Roll back a partial append before retrying.
                                out.seek(original_size)
                                out.truncate()
                                raise

                    except OSError:
                        message.nack()
                        return

                if tracker.add(seq):
                    try:
                        vehicle_id = int(payload['VEHICLE_ID'])
                        trip_id = int(payload['EVENT_NO_TRIP'])

                        breadcrumb_time = (
                            datetime.strptime(payload['OPD_DATE'],'%d%b%Y:%H:%M:%S',)
                            + timedelta(seconds=int(payload['ACT_TIME']))
                        )
                    except (KeyError, TypeError, ValueError, OverflowError):
                        tracker.bad_records += 1
                    else:
                        tracker.vehicles.add(vehicle_id)
                        tracker.trips.add(trip_id)

                        if (tracker.earliest is None or breadcrumb_time < tracker.earliest):
                            tracker.earliest = breadcrumb_time

                        if (tracker.latest is None or breadcrumb_time > tracker.latest):
                            tracker.latest = breadcrumb_time

    message.ack()

def compress_backup(tracker):
    # Create an empty file if this run contained zero breadcrumbs.
    with open(tracker.file_path, 'ab'):
        pass

    tracker.uncompressed_bytes = os.path.getsize(tracker.file_path)
    temporary_path = tracker.compressed_path + '.tmp'

    # Stream the file through gzip; do not load the whole file.
    with open(tracker.file_path, 'rb') as source:
        with gzip.open(temporary_path, 'wb') as destination:
            shutil.copyfileobj(source, destination)

    os.replace(temporary_path, tracker.compressed_path)
    tracker.compression_finished = datetime.now(timezone.utc)
    tracker.compression_finished_mono = time.monotonic()

    # Keep the compressed backup, removing the uncompressed copy.
    os.remove(tracker.file_path)

def print_summary(run_id, tracker):
    active = (
        tracker.last_mono - tracker.first_mono
        if tracker.first_mono is not None
        else 0.0
    )

    def format_time(value):
        return value.isoformat() if value is not None else 'N/A'

    print(f'\n--- Backup Summary ({SUBSCRIPTION_ID}) ---')
    print(f'Run ID                 : {run_id}')
    print(
        f'First received (UTC)   : '
        f'{format_time(tracker.first_received)}'
    )
    print(
        f'Last received (UTC)    : '
        f'{format_time(tracker.last_received)}'
    )
    print(f'Active receive time    : {active:.3f}s')
    print(f'Unique received        : {tracker.unique}')

    if active > 0:
        print(f'Throughput             : {tracker.unique / active:.2f} bc/s')
    else:
        print('Throughput             : N/A')

    print(f'Unique vehicles        : {len(tracker.vehicles)}')
    print(f'Earliest breadcrumb    : {format_time(tracker.earliest)}')
    print(f'Latest breadcrumb      : {format_time(tracker.latest)}')
    print(f'Unique trips           : {len(tracker.trips)}')
    print(f'Expected records       : {tracker.total}')
    print(f'Duplicates             : {tracker.duplicates}')
    print(f'Missing                : {tracker.missing}')
    print(f'Invalid record fields  : {tracker.bad_records}')
    print(
        f'Compression finished (UTC): '
        f'{format_time(tracker.compression_finished)}'
    )
    print(f'Bytes before compression: {tracker.uncompressed_bytes}')
    print(f'Compressed backup       : {tracker.compressed_path}')
    backup_walltime = (
        tracker.compression_finished_mono - tracker.first_mono
        if tracker.first_mono is not None
        else 0.0
    )

    print(f'Backup WALLTIME        : {backup_walltime:.3f}s')

    if backup_walltime > 0:
        print(
            f'Backup THROUGHPUT      : '
            f'{tracker.unique / backup_walltime:.2f} bc/s'
        )
    else:
        print('Backup THROUGHPUT      : N/A')
    print(flush=True)


def listen_continuously(streaming_pull):
    global untracked, bad_payloads

    while not streaming_pull.done():
        time.sleep(0.5)
        ready = []

        with lock:
            now = time.monotonic()

            for run_id, tracker in runs.items():
                if (
                    tracker.total is not None
                    and not tracker.finalizing
                    and now - tracker.last_activity >= IDLE_TIMEOUT
                ):
                    tracker.finalizing = True
                    ready.append((run_id, tracker))

        for run_id, tracker in ready:
            try:
                compress_backup(tracker)
            except OSError as exc:
                print(
                    f'Backup compression failed for {run_id}: {exc}; '
                    f'retrying after {IDLE_TIMEOUT}s',
                    flush=True,
                )

                with lock:
                    tracker.finalizing = False
                    tracker.last_activity = time.monotonic()

                continue

            with lock:
                del runs[run_id]
                closed_runs[run_id] = None

                while len(closed_runs) > CLOSED_RUN_LIMIT:
                    del closed_runs[next(iter(closed_runs))]

                invalid_counts = (untracked, bad_payloads)
                untracked = 0
                bad_payloads = 0

            print_summary(run_id, tracker)

            print(
                f'Invalid messages since previous report: '
                f'untracked={invalid_counts[0]}, '
                f'bad JSON={invalid_counts[1]}',
                flush=True,
            )


print(f'Listening continuously on subscription: {SUBSCRIPTION_ID}')
print(f'Reporting quiet period: {IDLE_TIMEOUT}s', flush=True)

streaming_pull = subscriber.subscribe(
    sub_path,
    callback=callback,
    flow_control=flow_control,
    await_callbacks_on_shutdown=True,
)

with subscriber:
    try:
        listen_continuously(streaming_pull)
        streaming_pull.result()  # Surface an error if the stream closes.
    except KeyboardInterrupt:
        print('\nBackup stopped by user.', flush=True)
    finally:
        streaming_pull.cancel()
        try:
            streaming_pull.result()
        except Exception:
            pass