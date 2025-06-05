from dagster import ScheduleDefinition
from ..jobs import transactions_job

# test_every_min = ScheduleDefinition(
#     job=transactions_job,
#     cron_schedule="* * * * *", # Run every minute, demo purposes only
#     # More at --> https://crontab.guru/
# )

monthly_schedule = ScheduleDefinition(
    job=transactions_job,
    cron_schedule="0 0 1 * *", # Run every month for monthly partitioning
    # More at --> https://crontab.guru/
)