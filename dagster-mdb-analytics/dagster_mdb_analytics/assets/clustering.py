from dagster_snowflake import SnowflakeResource
from dagster import asset, AutoMaterializePolicy
from ..partitions import monthly_partition

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from yellowbrick.cluster import KElbowVisualizer

# ===== DATA PREPARATION ===== #

@asset(deps=['dlt_mongodb_accounts', 'dlt_mongodb_transactions'])
def account_transaction_features(snowflake: SnowflakeResource) -> pd.DataFrame:
    """
    Join accounts and transactions to prepare training data for clustering tasks
    """
    query = """
        WITH base_accounts AS (
            SELECT 
                ACCOUNT_ID,
                LIMIT::INT AS CREDIT_LIMIT
            FROM DAGSTER_DB.ANALYTICS.ACCOUNTS a
            INNER JOIN DAGSTER_DB.ANALYTICS.ACCOUNTS__PRODUCTS ap ON a._DLT_ID = ap._DLT_PARENT_ID
        ),
        transaction_agg AS (
            SELECT 
                ACCOUNT_ID,
                COUNT(*) AS NUM_TRANSACTIONS,
                SUM(AMOUNT) AS TOTAL_VOLUME,
                SUM(TOTAL::FLOAT) AS TOTAL_VALUE,
                AVG(PRICE::FLOAT) AS AVG_PRICE
            FROM DAGSTER_DB.ANALYTICS.TRANSACTIONS t
            INNER JOIN DAGSTER_DB.ANALYTICS.TRANSACTIONS__TRANSACTIONS tt ON t._DLT_ID = tt._DLT_PARENT_ID
            GROUP BY ACCOUNT_ID
        )
        SELECT 
            a.ACCOUNT_ID,
            a.CREDIT_LIMIT,
            t.NUM_TRANSACTIONS,
            t.TOTAL_VOLUME,
            t.TOTAL_VALUE,
            t.AVG_PRICE
        FROM base_accounts a
        LEFT JOIN transaction_agg t ON a.ACCOUNT_ID = t.ACCOUNT_ID
    """

    with snowflake.get_connection() as conn:
        df = pd.read_sql(query, conn)

    df.fillna(0, inplace=True)  # fill missing values with 0
    df.to_csv('data_clustering_task/account_transaction_features.csv', index=False)
    return df

# ===== FEATURE ENGINEERING ===== #
@asset(deps=['account_transaction_features'])
def standardized_features() -> pd.DataFrame:
    """
    Perform feature scaling on the extracted features to prepare for clustering.
    """
    df = pd.read_csv('data_clustering_task/account_transaction_features.csv')

    features = ['CREDIT_LIMIT', 'NUM_TRANSACTIONS', 'TOTAL_VOLUME', 'TOTAL_VALUE', 'AVG_PRICE']
    scaler = StandardScaler()
    scaled_array = scaler.fit_transform(df[features])

    scaled_df = pd.DataFrame(scaled_array, columns=features)
    scaled_df['ACCOUNT_ID'] = df['ACCOUNT_ID']

    scaled_df.to_csv('data_clustering_task/standardized_features.csv', index=False)
    return scaled_df

# ===== FINDING ELBOWS ===== #
@asset(deps=['standardized_features'])
def elbow_chart():
    """
    Use Yellowbrick's KElbowVisualizer to determine the optimal number of clusters
    """
    df = pd.read_csv('data_clustering_task/standardized_features.csv')
    X = df.drop(columns=['ACCOUNT_ID'])

    model = KMeans(n_init=10, random_state=42)
    visualizer = KElbowVisualizer(model, k=(2, 10), metric='distortion', timings=False)

    visualizer.fit(X)
    visualizer.show(outpath='data_clustering_task/elbow_chart.png')


# ===== ACCOUNT SEGMENTATION ===== #
@asset(deps=['standardized_features'])
def account_transaction_clusters() -> pd.DataFrame:
    """
    Apply KMeans clustering on standardized features to segment accounts into distinct groups.
    """
    df = pd.read_csv('data_clustering_task/standardized_features.csv')
    X = df.drop(columns=['ACCOUNT_ID'])

    # Dynamic change n_clusters based on elbow chart (as data_clustering_task/elbow_chart.png shows the optimal number of clusters = 5) 
    kmeans = KMeans(n_clusters=5, n_init=10, random_state=42)  
    df['CLUSTER'] = kmeans.fit_predict(X)

    df.to_csv('data_clustering_task/account_transaction_clusters.csv', index=False)
    return df