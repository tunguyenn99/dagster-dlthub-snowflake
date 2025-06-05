from dagster import asset, Config
from dagster_snowflake import SnowflakeResource

import pandas as pd
import matplotlib.pyplot as plt

from prophet import Prophet  

class AdhocConfig(Config):
    filename: str
    sensor_data: str # This will affect how the sensor operates + File format in the datalake_example JSON files

@asset(
    deps=['dlt_mongodb_transactions']
)
def adhoc_daily_transaction_forecast(config: AdhocConfig, snowflake: SnowflakeResource) -> None:
    """
    Adhoc time series forecast: Predict daily transaction volume using Prophet
    """

    query = """
        SELECT 
            tt.DATE::DATE AS DS,
            COUNT(*) AS Y
        FROM DAGSTER_DB.ANALYTICS.TRANSACTIONS__TRANSACTIONS tt
        GROUP BY ds
        ORDER BY ds
    """

    with snowflake.get_connection() as conn:
        df = pd.read_sql(query, conn)

    df.columns = df.columns.str.lower()

    # Prophet expects columns: ds (date), y (value)
    model = Prophet()
    model.fit(df)

    future = model.make_future_dataframe(periods=30)  # forecast 30 days ahead
    forecast = model.predict(future)

    # Save forecast and plot
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv("data_adhoc/forecast_transaction_volume.csv", index=False)

    fig = model.plot(forecast)
    plt.title("Forecast: Daily Transaction Volume (Next 30 Days)")
    plt.xlabel("Date")
    plt.ylabel("Transactions")
    plt.tight_layout()
    plt.savefig("data_adhoc/forecast_transaction_volume.png")
