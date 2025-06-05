from dagster_snowflake import SnowflakeResource
from dagster import asset, AutoMaterializePolicy
from ..partitions import monthly_partition

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

# ===== DATA PREPARATION ===== #
@asset(deps=['dlt_mongodb_transactions'])
def transaction_features_for_regression(snowflake: SnowflakeResource) -> pd.DataFrame:
    """
    Extract relevant features from transactions for regression task.
    """
    query = """
        SELECT 
            ACCOUNT_ID,
            DATE::DATE AS TRANSACTION_DATE,
            TRANSACTION_CODE AS TYPE,
            AMOUNT::FLOAT AS VOLUME,
            PRICE::FLOAT AS UNIT_PRICE,
            TOTAL::FLOAT AS TOTAL_VALUE
        FROM DAGSTER_DB.ANALYTICS.TRANSACTIONS t
        INNER JOIN DAGSTER_DB.ANALYTICS.TRANSACTIONS__TRANSACTIONS tt ON t._DLT_ID = tt._DLT_PARENT_ID
    """

    with snowflake.get_connection() as conn:
        df = pd.read_sql(query, conn)

    df['TYPE'] = df['TYPE'].str.lower()
    df = df[df['TYPE'].isin(['buy', 'sell'])]
    df.dropna(inplace=True)
    df.to_csv('data_regression_task/transaction_features.csv', index=False)
    return df

# ===== MODEL TRAINING ===== #
@asset(deps=['transaction_features_for_regression'])
def regression_model_per_type() -> None:
    """
    Train a linear regression model separately for each transaction type: 'buy' and 'sell'.
    """
    df = pd.read_csv('data_regression_task/transaction_features.csv')
    results = []

    for t_type in ['buy', 'sell']:
        sub_df = df[df['TYPE'] == t_type]

        X = sub_df[['VOLUME', 'UNIT_PRICE']]
        y = sub_df['TOTAL_VALUE']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = LinearRegression()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results.append({
            'TYPE': t_type,
            'COEF_VOLUME': model.coef_[0],
            'COEF_UNIT_PRICE': model.coef_[1],
            'INTERCEPT': model.intercept_,
            'MSE': mse,
            'R2': r2
        })

        # Plot prediction
        plt.figure(figsize=(6, 4))
        sns.scatterplot(x=y_test, y=y_pred)
        plt.xlabel('Actual Total Value')
        plt.ylabel('Predicted Total Value')
        plt.title(f'{t_type.capitalize()} Transactions Prediction')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f'data_regression_task/{t_type}_prediction.png')

    pd.DataFrame(results).to_csv('data_regression_task/regression_summary.csv', index=False)


@asset(deps=['transaction_features_for_regression'])
def regression_model_per_type_post_2015() -> None:
    """
    Train a linear regression model separately for each transaction type ('buy' and 'sell') 
    using data up to 2015, then predict on later data.
    """
    df = pd.read_csv('data_regression_task/transaction_features.csv')
    df['TRANSACTION_DATE'] = pd.to_datetime(df['TRANSACTION_DATE'])

    results = []

    for t_type in ['buy', 'sell']:
        sub_df = df[df['TYPE'] == t_type]

        # Split by date
        train_df = sub_df[sub_df['TRANSACTION_DATE'] <= '2015-12-31']
        test_df = sub_df[sub_df['TRANSACTION_DATE'] > '2015-12-31']

        X_train = train_df[['VOLUME', 'UNIT_PRICE']]
        y_train = train_df['TOTAL_VALUE']

        X_test = test_df[['VOLUME', 'UNIT_PRICE']]
        y_test = test_df['TOTAL_VALUE']

        model = LinearRegression()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        results.append({
            'TYPE': t_type,
            'COEF_VOLUME': model.coef_[0],
            'COEF_UNIT_PRICE': model.coef_[1],
            'INTERCEPT': model.intercept_,
            'MSE': mse,
            'R2': r2
        })

        # Plot prediction
        plt.figure(figsize=(6, 4))
        sns.scatterplot(x=y_test, y=y_pred)
        plt.xlabel('Actual Total Value')
        plt.ylabel('Predicted Total Value')
        plt.title(f'{t_type.capitalize()} Transactions Prediction (post-2015)')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f'data_regression_task/{t_type}_prediction_post_2015.png')

    pd.DataFrame(results).to_csv('data_regression_task/regression_summary_post_2015.csv', index=False)
