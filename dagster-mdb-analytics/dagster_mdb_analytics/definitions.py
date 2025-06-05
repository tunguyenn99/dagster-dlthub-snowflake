from dagster import Definitions, load_assets_from_modules

from .assets import mongodb, transactions, classification, regression, adhoc
from .partitions import monthly_partition
from .resources import snowflake_resource, dlt_resource
from .jobs import transactions_job, adhoc_job
from .schedules import monthly_schedule
from .sensors import adhoc_sensor


mongodb_assets = load_assets_from_modules([mongodb])
transaction_assets = load_assets_from_modules([transactions], group_name="transactions")
classification_assets = load_assets_from_modules([classification], group_name="classification")
regression_assets = load_assets_from_modules([regression], group_name="regression")
adhoc_assets = load_assets_from_modules([adhoc], group_name="adhoc")

defs = Definitions(
    assets=[*mongodb_assets,
            *transaction_assets,
            *classification_assets,
            *regression_assets,
            *adhoc_assets
            ],
    resources={
        "dlt": dlt_resource,
        "snowflake": snowflake_resource
    },
    jobs=[transactions_job, adhoc_job],
    schedules=[monthly_schedule],
    sensors=[adhoc_sensor]
)