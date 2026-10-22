# publisher.py
import os
import sys
import threading
import time
import uuid
import json
from datetime import datetime, timezone

import requests
import google.auth
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import (
    BatchSettings,
    LimitExceededBehavior,
    PublisherOptions,
    PublishFlowControl,
)

start = time.perf_counter()

VEHICLE_IDS_LARGE = [
    3164, 3026, 4020, 3602, 2910, 3214, 3963, 3601, 3307, 3265,
    3734, 4531, 3035, 4011, 3133, 3520, 4013, 3518, 3917, 3543,
    3310, 3748, 3120, 3415, 3606, 3801, 4003, 3002, 3916, 3324,
    3959, 3960, 3216, 3241, 4054, 4528, 4202, 2909, 3513, 4059,
    4044, 4069, 4016, 3736, 3043, 3709, 3551, 3205, 2938, 3161,
    3903, 3254, 3604, 3706, 4040, 3159, 4038, 4007, 3703, 3262,
    3105, 4212, 3757, 3033, 3936, 3646, 3941, 3914, 3643, 4067,
    3167, 3557, 3555, 3169, 3249, 3525, 3207, 3330, 3243, 3912,
    3166, 4216, 4239, 3422, 3932, 3123, 3136, 4201, 3538, 2913,
    3106, 3524, 3114, 4223, 3011, 3905, 3246, 3545, 4234, 3158,
    4530, 3021, 3554, 3741, 4504, 3309, 3529, 3009, 4018, 3633,
    2935, 3115, 2924, 2937, 3915, 4022, 4207, 3922, 3744, 3752,
    4047, 3155, 3233, 2905, 4006, 3632, 3126, 3267, 3954, 3943,
    3712, 3938, 3410, 3240, 3926, 4213, 3747, 3227, 2919, 3314,
    3213, 4028, 3902, 3534, 3920, 3562, 2917, 3327, 3603, 4012,
    4035, 3220, 3930, 3229, 3958, 3144, 4004, 3040, 3925, 3745,
    4517, 3403, 3572, 3510, 3616, 3151, 3750, 4233, 3259, 3720,
    2911, 3020, 3508, 3247, 4023, 3515, 3010, 4034, 3038, 3964,
    4527, 3311, 3306, 4019, 3947, 3904, 4211, 3612, 2936, 3713,
    4225, 4215, 4024, 3553, 4055, 3573, 4026, 3556, 4238, 3540,
    3029, 3702, 3710, 3407, 3253, 3108, 3135, 3638, 3016, 3042,
    3727, 3950, 3104, 2921, 3522, 4224, 3506, 3058, 3257, 4304,
    3402, 3113, 4217, 3245, 3636, 2926, 4001, 3957, 2934, 3921,
    3412, 3420, 3206, 3605, 3235, 3059, 4227, 3305, 3933, 4226,
    3927, 3312, 3212, 3222, 3157, 3909, 3251, 3931, 3521, 3117,
]

API_URL = 'https://busdata.cs.pdx.edu/api/getBreadCrumbs'
PROJECT_ID = None  # Auto-detected on the VM; set only if needed.
TOPIC_ID = 'bc_topic'

batch_settings = BatchSettings(
    max_messages=100,
    max_latency=0.05,
)

flow_control = PublishFlowControl(
    message_limit=10_000,
    byte_limit=20 * 1024 * 1024,
    limit_exceeded_behavior=LimitExceededBehavior.BLOCK,
)


def fetch_breadcrumbs(session, vehicle_id, timeout=10, attempts=3):
    """Return the list of breadcrumb records for one vehicle.

    Raises requests.HTTPError for an HTTP error status (usually: no data for
    that vehicle today) and ValueError for an unexpected response shape.
    Connection errors and timeouts are retried before giving up.
    """
    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(API_URL, params={'vehicle_id': vehicle_id}, timeout=timeout)
            break
        except (requests.ConnectionError, requests.Timeout):
            if attempt == attempts:
                raise
            time.sleep(2 * attempt)
    resp.raise_for_status()
    data = resp.json()
    records = data.get('results') if isinstance(data, dict) else data
    if not isinstance(records, list):
        raise ValueError(f'unexpected response shape: {type(data).__name__}')
    return records


def resolve_project_id(explicit=None):
    project = explicit or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project:
        _, project = google.auth.default()
    if not project:
        raise SystemExit('Could not determine the GCP project ID. Set PROJECT_ID '
                         'in this script or export GOOGLE_CLOUD_PROJECT.')
    return project


if not VEHICLE_IDS_LARGE:
    raise SystemExit('Add your assigned vehicle IDs to VEHICLE_IDS_LARGE.')

publisher = pubsub_v1.PublisherClient(
    batch_settings=batch_settings,
    publisher_options=PublisherOptions(flow_control=flow_control),
)

topic_path = publisher.topic_path(
    resolve_project_id(PROJECT_ID),
    TOPIC_ID,
)

# Fail early if the topic is missing or permissions are insufficient.
publisher.get_topic(topic=topic_path)

RUN_ID = time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex

# Publish callbacks run on background threads.
done_cv = threading.Condition()
ok = 0
errors = 0
error_examples = []
final_publish_time = None
final_submit_time = None

def on_publish_done(future):
    global ok, errors, final_publish_time

    exc = future.exception()

    with done_cv:
        if exc is None:
            ok += 1
            final_publish_time = datetime.now(timezone.utc)
        else:
            errors += 1
            if len(error_examples) < 5:
                error_examples.append(repr(exc))

        done_cv.notify_all()


vehicle_ids = list(dict.fromkeys(VEHICLE_IDS_LARGE))
published = 0
with_data = 0
api_errors = 0
api_start_time = None

print(f'Run ID        : {RUN_ID}')
print(f'Publishing to : {topic_path}')
print(f'Vehicles      : {len(vehicle_ids)}')

try:
    with requests.Session() as session:
        for vid in vehicle_ids:
            if api_start_time is None:
                api_start_time = datetime.now(timezone.utc)

            try:
                records = fetch_breadcrumbs(session, vid)
            except (requests.RequestException, ValueError) as exc:
                api_errors += 1
                with done_cv:
                    if len(error_examples) < 5:
                        error_examples.append(f'Vehicle {vid}: {exc}')
                continue

            if records:
                with_data += 1

            # Publish each breadcrumb as a separate JSON message.
            for record in records:
                try:
                    data = json.dumps(
                        record, separators=(',', ':')
                    ).encode('utf-8')

                    future = publisher.publish(
                        topic_path,
                        data,
                        run_id=RUN_ID,
                        seq=str(published),
                    )
                    final_submit_time = datetime.now(timezone.utc)
                except Exception as exc:
                    with done_cv:
                        errors += 1
                        if len(error_examples) < 5:
                            error_examples.append(repr(exc))
                else:
                    future.add_done_callback(on_publish_done)

                # Count all attempts, including immediate failures.
                published += 1

            # Release this vehicle's response before fetching another.
            del records
            time.sleep(0.1)

finally:
    # Wait until every breadcrumb attempt has succeeded or failed.
    with done_cv:
        while ok + errors < published:
            done_cv.wait(timeout=1)

    try:
        summary = {
            'expected_total': published,
        }

        publisher.publish(
            topic_path,
            json.dumps(summary).encode('utf-8'),
            run_id=RUN_ID,
            message_type='run_summary',
        ).result(timeout=60)

        all_messages_finished = datetime.now(timezone.utc)

    finally:
        publisher.stop()
elapsed = time.perf_counter() - start

print('\n--- Publisher Summary ---')
print(f'Run ID                    : {RUN_ID}')
print(f'Publishing attempts       : {published}')
print(
    f'API access began (UTC)    : '
    f'{api_start_time.isoformat() if api_start_time else "N/A"}'
)
print(
    f'Final message submitted   : '
    f'{final_submit_time.isoformat() if final_submit_time else "N/A"}'
)
print(
    f'Final publish confirmed   : '
    f'{final_publish_time.isoformat() if final_publish_time else "N/A"}'
)
print(f'Elapsed wall-clock time   : {elapsed:.3f}s')
print(f'Breadcrumbs published     : {ok}')
print(f'Publishing throughput     : {ok / elapsed:.2f} breadcrumbs/s')
print(f'Unique vehicles with data : {with_data}')
print(f'Publishing errors         : {errors}')
print(f'API errors                : {api_errors}')
print(f'End timestamp             : {all_messages_finished.isoformat()}')


for error in error_examples:
    print(f'Example error             : {error}')

sys.exit(2 if errors or api_errors else 0)