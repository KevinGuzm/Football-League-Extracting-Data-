# Data Engineering Project: Extracting and Analyzing Data from Major European Football Leagues

This project uses Python and several libraries, including `pandas`, `time`, `random`, `os`, and `datetime` to extract, process, and analyze data from the major European football leagues.

## Data Extraction and Processing

The script defines two main functions: `get_data` and `data_processing`.

The `get_data` function takes a URL and a league name as arguments. It uses the `pandas` library to read HTML data from the provided URL. The data is processed and cleaned, and two new columns are added: 'LIGA', which contains the league name, and 'CREATED_AT', which contains the current date. The function returns a pandas DataFrame with the processed data.

The `data_processing` function takes a DataFrame as an argument. This function calls the `get_data` function for each league and URL in the provided DataFrame. The resulting DataFrames are concatenated into a single DataFrame, which is returned.

These functions allow for the extraction and processing of data from various football leagues from provided URLs. The processed data is ready to be loaded into a database or used for further analysis.

## Workflow Automation with Apache Airflow

In addition, the project uses Apache Airflow to automate and schedule the workflow of extracting, processing, and loading (ETL) the data. An Airflow Directed Acyclic Graph (DAG) is defined that consists of three main tasks:

1. `EXTRACT_FOTBALL_DATA`: This task uses a Python operator to call the `extract_info` function, which extracts and processes the data using the previously defined functions.
2. `upload_data_stage`: This task uses a Snowflake operator to load the processed data into a stage table in Snowflake.
3. `ingest_table`: This task also uses a Snowflake operator to ingest the data from the stage table to the final table in Snowflake.

These tasks are executed in sequence, meaning each task depends on the success of the previous task.

## Summary

In summary, this project demonstrates a solid understanding of modern data engineering techniques, including data extraction and processing with Python and pandas, workflow automation with Apache Airflow, and data storage and analysis with Snowflake.
