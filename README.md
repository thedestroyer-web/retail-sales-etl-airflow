# retail-sales-etl-airflow
retail sales etl pipeline using python,pandas,mysql,and apache airflow

# Retail Sales ETL Pipeline using Apache Airflow

## Project Overview

This project demonstrates an ETL (Extract, Transform, Load) pipeline
built using Python, Pandas, MySQL, SQLAlchemy, and Apache Airflow.

The pipeline extracts raw retail sales data from MySQL, cleans and
transforms the data using Pandas, validates the records, and loads
the cleaned data into a destination MySQL table.

Apache Airflow is used to orchestrate the ETL workflow.

## Technologies Used

- Python
- Pandas
- MySQL
- SQLAlchemy
- Apache Airflow
- Docker

## ETL Workflow

### 1. Extract

Extracted raw sales records from the MySQL table `raw_sales`
using Pandas and SQLAlchemy.

### 2. Transform

- Standardized product names
- Removed records with invalid quantities
- Removed records with invalid prices
- Created a `total_amount` column

### 3. Validate

Checked the transformed data for:

- Missing values
- Duplicate order IDs
- Invalid quantities
- Invalid prices

### 4. Load

Loaded the cleaned data into the MySQL table `cleaned_sales`.

## Airflow Tasks

The DAG contains the following tasks:

1. start_pipeline
2. test_mysql_connection
3. extract_data
4. transform_data
5. load_data

The tasks execute sequentially using Airflow task dependencies.

## Project Results

- Raw records extracted: 15
- Cleaned records loaded: 13

Two records with invalid quantities or prices were removed
during the transformation process.

## Learning Outcomes

Through this project, I practiced:

- Building an ETL pipeline
- Connecting Python with MySQL
- Data cleaning and validation using Pandas
- Loading DataFrames into MySQL
- Using Airflow PythonOperator
- Passing data between tasks using XCom
- Orchestrating an ETL workflow with Apache Airflow
