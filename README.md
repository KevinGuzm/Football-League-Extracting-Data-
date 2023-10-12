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

==========================================================================
# Proyecto de Ingeniería de Datos: Extracción y Análisis de Datos de las Principales Ligas de Fútbol Europeas

Este proyecto utiliza Python y varias bibliotecas, incluyendo `pandas`, `time`, `random`, `os` y `datetime` para extraer, procesar y analizar datos de las principales ligas de fútbol europeas.

## Extracción y Procesamiento de Datos

El script define dos funciones principales: `get_data` y `data_processing`.

La función `get_data` toma una URL y el nombre de una liga como argumentos. Utiliza la biblioteca `pandas` para leer los datos HTML de la URL proporcionada. Los datos se procesan y se limpian, y se añaden dos nuevas columnas: 'LIGA', que contiene el nombre de la liga, y 'CREATED_AT', que contiene la fecha actual. La función devuelve un DataFrame de pandas con los datos procesados.

La función `data_processing` toma un DataFrame como argumento. Esta función llama a la función `get_data` para cada liga y URL en el DataFrame proporcionado. Los DataFrames resultantes se concatenan en un solo DataFrame, que se devuelve.

Estas funciones permiten extraer y procesar datos de varias ligas de fútbol a partir de URLs proporcionadas. Los datos procesados están listos para ser cargados en una base de datos o utilizados para análisis posteriores.

## Automatización del Flujo de Trabajo con Apache Airflow

Además, el proyecto utiliza Apache Airflow para automatizar y programar el flujo de trabajo de extracción, procesamiento y carga (ETL) de los datos. Se define un DAG (Directed Acyclic Graph) en Airflow que consta de tres tareas principales:

1. `EXTRACT_FOTBALL_DATA`: Esta tarea utiliza un operador Python para llamar a la función `extract_info`, que extrae y procesa los datos utilizando las funciones definidas anteriormente.
2. `upload_data_stage`: Esta tarea utiliza un operador Snowflake para cargar los datos procesados en una tabla stage en Snowflake.
3. `ingest_table`: Esta tarea también utiliza un operador Snowflake para ingerir los datos desde la tabla stage a la tabla final en Snowflake.

Estas tareas se ejecutan en secuencia, lo que significa que cada tarea depende del éxito de la tarea anterior.

## Resumen

En resumen, este proyecto demuestra una sólida comprensión de las técnicas modernas de ingeniería de datos, incluyendo la extracción y procesamiento de datos con Python y pandas, la automatización del flujo de trabajo con Apache Airflow, y el almacenamiento y análisis de datos con Snowflake.
