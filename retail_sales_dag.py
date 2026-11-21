from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
from io import StringIO


# ==================================
# STEP 1: START FUNCTION
# ==================================

def welcome_message():
    print("Welcome to Retail Sales ETL Pipeline!")


# ==================================
# STEP 2: MYSQL CONNECTION FUNCTION
# ==================================

def test_mysql_connection():

    engine = create_engine(
        "mysql+mysqlconnector://root:******@host.docker.internal:3306/retail_sales_db"
    )

    with engine.connect() as conn:

        result = conn.execute(
            text("SELECT COUNT(*) FROM raw_sales")
        )

        total_records = result.scalar()

        print("Successfully connected to MySQL!")
        print("Total records:", total_records)

    engine.dispose()


# ==================================
# STEP 3: EXTRACTION FUNCTION
# ==================================

def extract_data():

    engine = create_engine(
        "mysql+mysqlconnector://root:******@host.docker.internal:3306/retail_sales_db"
    )

    query = "SELECT * FROM raw_sales"

    raw_df = pd.read_sql(query, engine)

    print("Data extracted successfully!")
    print("Total records:", len(raw_df))
    print(raw_df)

    engine.dispose()

    return raw_df.to_json(
        orient="records",
        date_format="iso"
    )




def transform_data(ti):

    # Receive extracted data from XCom
    raw_json = ti.xcom_pull(task_ids="extract_data")

    # Convert JSON back into a DataFrame
    raw_df = pd.read_json(
        StringIO(raw_json),
        orient="records"
    )

    print("Data received successfully!")
    print("Original records:", len(raw_df))

    # Create a working copy
    cleaned_df = raw_df.copy()

    # Standardize product names
    cleaned_df["product_name"] = (
        cleaned_df["product_name"]
        .str.strip()
        .str.upper()
    )

    # Remove invalid quantities and prices
    cleaned_df = cleaned_df[
        (cleaned_df["quantity"] > 0) &
        (cleaned_df["price"] > 0)
    ].copy()

    # Calculate total amount
    cleaned_df["total_amount"] = (
        cleaned_df["quantity"] * cleaned_df["price"]
    )
    # Validate transformed data

    print("Missing values:")
    print(cleaned_df.isnull().sum())

    print("Duplicate order IDs:")
    print(cleaned_df["order_id"].duplicated().sum())

    print("Invalid quantities:")
    print((cleaned_df["quantity"] <= 0).sum())

    print("Invalid prices:")
    print((cleaned_df["price"] <= 0).sum())

    print("Final records:", len(cleaned_df))

    print("Transformation completed!")
    print("Cleaned records:", len(cleaned_df))
    print(cleaned_df)

    # Send cleaned data to the next task
    return cleaned_df.to_json(
        orient="records",
        date_format="iso"
    )  


def load_data(ti):

    # Receive cleaned data from transform_data task
    cleaned_records = ti.xcom_pull(
        task_ids="transform_data"
    )

    # Convert received records into DataFrame
    cleaned_df=pd.read_json(
        StringIO(cleaned_records),
        orient="records"
    )

    # Connect to MySQL
    engine = create_engine(
        "mysql+mysqlconnector://root:*******@host.docker.internal:3306/retail_sales_db"
    )

    # Load data into MySQL
    cleaned_df.to_sql(
        name="cleaned_sales",
        con=engine,
        if_exists="replace",
        index=False
    )

    print("Data loaded successfully!")
    print("Total records loaded:", len(cleaned_df))

    engine.dispose()


# ==================================
# STEP 4: DEFINE THE DAG
# ==================================

with DAG(
    dag_id="retail_sales_pipeline",
    start_date=datetime(2026, 11, 18),
    schedule=None,
    catchup=False
) as dag:

    # First task
    start_task = PythonOperator(
        task_id="start_pipeline",
        python_callable=welcome_message
    )

    # Second task
    mysql_test_task = PythonOperator(
        task_id="test_mysql_connection",
        python_callable=test_mysql_connection
    )

    # Third task
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

     #fourth task
    transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data
)   



    load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data
)

    # Define execution order
    start_task >> mysql_test_task >> extract_task >> transform_task >> load_task