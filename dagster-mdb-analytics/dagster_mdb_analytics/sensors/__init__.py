from dagster import (
    RunRequest,
    SensorResult,
    sensor
)

import os
import json
from ..jobs import adhoc_job

@sensor(
    job=adhoc_job
)
def adhoc_sensor(context):
    PATH_TO_REQUESTS = os.path.join(os.path.dirname(__file__), "../../", "datalake_example")

    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    runs_to_request = []

    for filename in os.listdir(PATH_TO_REQUESTS):
        file_path = os.path.join(PATH_TO_REQUESTS, filename)
        if filename.endswith(".json") and os.path.isfile(file_path):
            
            last_modified = os.path.getmtime(file_path)

            current_state[filename] = last_modified

            # if the file is new or has been modified since the last run, add it to the request queue
            if filename not in previous_state or previous_state[filename] != last_modified:
                with open(file_path, "r") as f:
                    request_config = json.load(f)
                    runs_to_request.append(RunRequest(
                        run_key=f"adhoc_daily_transaction_forecast_{filename}_{last_modified}",
                        run_config={
                            "ops": {
                                "adhoc_daily_transaction_forecast": {
                                    "config": {
                                        "filename": filename,
                                        **request_config
                                    }
                                }
                            }
                        }
                    ))

    return SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(current_state)
    )