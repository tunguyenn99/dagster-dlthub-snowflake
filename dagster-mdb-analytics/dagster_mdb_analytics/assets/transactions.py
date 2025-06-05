from dagster_snowflake import SnowflakeResource
from dagster import asset, AutoMaterializePolicy
from ..partitions import monthly_partition

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter

@asset(
    deps=["dlt_mongodb_transactions"]
)
def cleaned_transactions(snowflake: SnowflakeResource) -> pd.DataFrame:
    query = """
        SELECT
            ACCOUNT_ID,
            DATE::DATE AS TRANSACTION_DATE,
            TRANSACTION_CODE AS TYPE,
            SYMBOL AS STOCK,
            AMOUNT::INT AS VOLUME,
            PRICE::FLOAT AS UNIT_PRICE,
            TOTAL::FLOAT AS TOTAL_VALUE
        FROM DAGSTER_DB.ANALYTICS.TRANSACTIONS t
        INNER JOIN DAGSTER_DB.ANALYTICS.TRANSACTIONS__TRANSACTIONS tt ON t._DLT_ID = tt._DLT_PARENT_ID 
    """

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()

    # Optionally clean/filter
    df = df[df["VOLUME"] > 0]  # exclude transactions <= 0 volume 
    df["TYPE"] = df["TYPE"].str.lower() # normalize transaction type to lowercase

    return df

@asset
def stock_transaction_summary_by_date(cleaned_transactions: pd.DataFrame) -> pd.DataFrame:
    summary = (
        cleaned_transactions
        .groupby(["TRANSACTION_DATE", "STOCK", "TYPE"], as_index=False)
        .agg(
            total_volume=("VOLUME", "sum"),
            total_value=("TOTAL_VALUE", "sum"),
            avg_price=("UNIT_PRICE", "mean"),
            transactions=("STOCK", "count")
        )
    )
    summary.to_csv('data/stock_transaction_summary_by_date.csv', index=False)

    return summary

@asset(
    deps=["stock_transaction_summary_by_date"],
    auto_materialize_policy=AutoMaterializePolicy.eager()
)
def top_selling_stocks_by_transaction_volume():
    """
    Generate a bar chart based on top 10 stocks by transaction volume
    """
    stock_summary = pd.read_csv('data/stock_transaction_summary_by_date.csv')
    top_selling_10_stocks = stock_summary[stock_summary['TYPE'] == 'sell'].groupby(by='STOCK').agg(TOTAL_VOLUME=('total_volume', 'sum')).sort_values(by='TOTAL_VOLUME', ascending=False).head(10)

    plt.figure(figsize=(10, 8))
    ax = sns.barplot(
        data=top_selling_10_stocks,
        x='TOTAL_VOLUME',
        y='STOCK',
        palette='Greens_r'
    )

    def format_number(x, pos):
        if x >= 1_000_000_000:
            return f'{x / 1_000_000_000:.1f}B'
        elif x >= 1_000_000:
            return f'{x / 1_000_000:.1f}M'
        elif x >= 1_000:
            return f'{x / 1_000:.1f}K'
        else:
            return f'{x:.0f}'

    for container in ax.containers:
        ax.bar_label(container, labels=[format_number(v, None) for v in container.datavalues])

    plt.xlabel('Transaction Volume')
    plt.ylabel('Stock')
    plt.title('Top Selling Stocks by Transaction Volume')

    plt.tight_layout()
    plt.savefig('data/top_selling_stocks_by_transaction_volume.png')

@asset(
    deps=["stock_transaction_summary_by_date"],
    auto_materialize_policy=AutoMaterializePolicy.eager()
)
def top_buying_stocks_by_transaction_volume():
    """
    Generate a bar chart based on top 10 stocks by transaction volume
    """
    stock_summary = pd.read_csv('data/stock_transaction_summary_by_date.csv')
    top_buying_10_stocks = stock_summary[stock_summary['TYPE'] == 'buy'].groupby(by='STOCK').agg(TOTAL_VOLUME=('total_volume', 'sum')).sort_values(by='TOTAL_VOLUME', ascending=False).head(10)

    plt.figure(figsize=(10, 8))
    ax = sns.barplot(
        data=top_buying_10_stocks,
        x='TOTAL_VOLUME',
        y='STOCK',
        palette='Greens_r'
    )

    def format_number(x, pos):
        if x >= 1_000_000_000:
            return f'{x / 1_000_000_000:.1f}B'
        elif x >= 1_000_000:
            return f'{x / 1_000_000:.1f}M'
        elif x >= 1_000:
            return f'{x / 1_000:.1f}K'
        else:
            return f'{x:.0f}'

    for container in ax.containers:
        ax.bar_label(container, labels=[format_number(v, None) for v in container.datavalues])

    plt.xlabel('Transaction Volume')
    plt.ylabel('Stock')
    plt.title('Top Buying Stocks by Transaction Volume')

    plt.tight_layout()
    plt.savefig('data/top_buying_stocks_by_transaction_volume.png')

# ============================== #

@asset(deps=["dlt_mongodb_accounts"])
def accounts_flattened_products(snowflake: SnowflakeResource) -> pd.DataFrame:
    """
    Flattened version of accounts and their associated products from MongoDB into one row per product per account.
    Useful for granular analysis of which product types are linked to which accounts.
    """    
    query = """
        SELECT 
            a.ACCOUNT_ID,
            a.LIMIT::INT AS CREDIT_LIMIT,
            ap.VALUE AS PRODUCT
        FROM DAGSTER_DB.ANALYTICS.ACCOUNTS a
        INNER JOIN DAGSTER_DB.ANALYTICS.ACCOUNTS__PRODUCTS ap 
            ON a._DLT_ID = ap._DLT_PARENT_ID
    """
    with snowflake.get_connection() as conn: #trying another way to connect to Snowflake
        df = pd.read_sql(query, conn)

    df['PRODUCT'] = df['PRODUCT'].str.strip()
    return df

@asset
def product_distribution(accounts_flattened_products: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate how many unique accounts are associated with each product type.
    Helps identify the most popular financial products across all users.
    """    
    product_counts = (
        accounts_flattened_products
        .groupby("PRODUCT")
        .agg(num_accounts=("ACCOUNT_ID", "nunique"))
        .sort_values("num_accounts", ascending=False)
        .reset_index()
    )
    product_counts.to_csv("data/product_distribution.csv", index=False)
    return product_counts

@asset
def average_limit_by_product(accounts_flattened_products: pd.DataFrame) -> pd.DataFrame:
    """
    Compute average, min, and max credit limits for each product type.
    Provides insight into which products are typically linked to high-value accounts.
    """    
    result = (
        accounts_flattened_products
        .groupby("PRODUCT", as_index=False)
        .agg(
            avg_credit_limit=("CREDIT_LIMIT", "mean"),
            min_limit=("CREDIT_LIMIT", "min"),
            max_limit=("CREDIT_LIMIT", "max"),
            accounts=("ACCOUNT_ID", "nunique")
        )
    )
    result.to_csv("data/avg_limit_by_product.csv", index=False)
    return result

@asset(deps=["product_distribution"])
def top_products_chart():
    """
    Generate a bar chart of the top 10 most common products by number of accounts.
    Uses the CSV exported by `product_distribution` and saves the chart as a PNG.
    """    
    df = pd.read_csv("data/product_distribution.csv")
    top = df.sort_values("num_accounts", ascending=False).head(10)

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=top, x="num_accounts", y="PRODUCT", palette="viridis")

    ax.bar_label(ax.containers[0])
    plt.title("Top Products by Number of Accounts")
    plt.xlabel("Number of Accounts")
    plt.ylabel("Product")
    plt.tight_layout()
    plt.savefig("data/top_products_by_account.png")