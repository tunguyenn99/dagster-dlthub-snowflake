from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets

import dlt
from ..mongodb import mongodb, mongodb_collection
# from pymongoarrow.schema import Schema 

# arrow_schema = Schema({})

analytics = mongodb(
    database='sample_analytics',
    # pymongoarrow_schema=arrow_schema
).with_resources(
    "accounts",
    # "customers",
    "transactions"
)

@dlt_assets(
    dlt_source=analytics,
    dlt_pipeline=dlt.pipeline(
        pipeline_name="local_mongo",
        destination='snowflake',
        dataset_name="analytics",
    ),
    name="mongodb",
    group_name="mongodb",
)
def dlt_asset_factory(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context, write_disposition="merge")