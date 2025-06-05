from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition

# https://docs.dagster.io/api/dagster/assets
transactions_job = define_asset_job(
    name="transactions_job",
    # partitions_def=monthly_partition,
    selection=AssetSelection.all() - AssetSelection.groups("mongodb") # Use groups instead of assets
)

adhoc_job = define_asset_job(
    name="adhoc_job",
    selection=AssetSelection.assets(["adhoc_daily_transaction_forecast"])
)