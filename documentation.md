# Table of Contents

- [Overview](#overview)
- [Data Lake Structure](#data-lake-structure)
    - [Landing Layer](#landing-layer)
    - [Bronze Layer](#bronze-layer)
    - [Silver Layer](#silver-layer)
    - [Golden Layer](#golden-layer)
- [Process](#process)
    - [etl_process](#etl-process)
        - [Reserved dataset column names](#reserved-dataset-column-names)
    - [helper](#helper-functions)
        - [logger](#logger)
        - [download_dataset](#download_dataset)
        - [create_spark_session](#create_spark_session)
        - [read_df_from_path](#read_df_from_path)
        - [write_df_to_path_as_parquet](#write_df_to_path_as_parquet)
- [Deployment](#deployment)
    - [Docker](#docker)
    - [Databricks](#databricks)

# Overview

This document provides a description of the processes and functions developed.

# Data Lake Structure

The data lake follows the Medallion Architecture ([source](https://www.databricks.com/glossary/medallion-architecture)), consisting of the following layers:

## Landing Layer

The landing layer stores initial ingested raw files.

## Bronze Layer

The bronze layer stores ingestion data. Each folder corresponds to a source dataset, and the data is partitioned.
		
## Silver Layer
The silver layer contains data with naming conventions applied, transformed columns, and no duplicates. Each folder represents a dataset, and the data is partitioned.

## Golden Layer
The golden layer stores highly refined and aggregated data for analytics, this is the consumption layer. Source data in the golden layer comes exclusively from the silver layer. Data partitioning is applied when possible.


# Process
The process involves two Python scripts:

- `etl_process.py`: Executes business requirements for ingestion, transformation, and final dataset creation.
- `helper.py`: Contains library functions used in `etl_process.py`. 



## etl_process
The `etl_process.py` script performs the following operations:
- Ingestion of Resources:
    - Download the source dataset from Kaggle into the landing layer.
    - Unzip the downloaded dataset within the same layer.
    - Delete the zip file.  
- Create and Start of the Spark Session.
- Creation of the Bronze Layer Dataset:
    - Read the source dataset from the landing container by creating a dataframe from the CSV file.
    - Add the columns `file_path`, `execution_datetime`, and partition columns `year`, `month`, and `day` to the dataframe.
    - Write the dataframe as Parquet in the bronze layer under the `sales_dataset` folder.
- Creation of the Silver Layer Dataset:
    - Read the dataset from the bronze layer by creating a dataframe.
    - Rename every column using snake case notation.
    - Add the columns `file_path` and `execution_datetime`.
    - Cast the `order_date` and `ship_date` columns to the date datatype.
    - Cast the `sales` column to the double datatype.
    - Ensure that the partition columns `month` and `day` are formatted as "two-digit string".
    - Write the dataframe as Parquet in the silver layer under the `sales_dataset` folder.
- Creation of the Golden Datasets:
    - Read the dataset from the silver layer by creating a dataframe.
    - Sales Dataset Creation:
        - Create the sales dataframe by selecting the necessary business columns from the silver dataframe.
        - Check if the sales dataset is available in the golden layer by reading it and creating a dataframe. If not, create an empty one.
        - Calculate the delta between the new and the old dataframe. If there are differences, append them to the golden layer's sales folder. The appended dataframe will include `file_path`, `execution_datetime`, renamed business columns, and partition columns `year`, `month`, and `day`.
    - Customer Dataset Creation:
        - Create the customer dataframe from the silver dataframe and add a column to hold the difference between today's date and the `order_date`.
        - Define conditions for quantities of orders within the last 5, 15, and 30 days.
        - Group by the requested business columns and calculate quantities of orders for the specified time periods. Also, calculate the total quantity of orders per customer.
        - Add the columns `file_path` and `execution_datetime`.
        - Select the necessary business columns and write the dataframe to the golden layer's customer folder.
- Stop the Spark Session.

### Reserved dataset column names
It is only right to make a note regarding two reserved column names for the datasets:
- `file_path`: this column stores the output of the function `pyspark.sql.functions.input_file_name` for the bronza and silver layer. However, on the golden layer, it exclusively stores the root path of the dataset folder. This choice was made to ensure consistency within the golden layer. Oftentimes, aggregations are executed on this layer, and as a result, a single record may reference multiple files. By adopting this approach, the hierarchy is maintained, albeit with slightly more flexibility for the other layers.
- `execution_datetime`: this column captures the timestamp of the execution.

## helper
The `helper.py` module provides the following functions:

### logger
The `logger` object instantiated in the helper module is configured to INFO level. 
In the event is needed to debug the level can be changed with the following command: `logger.setLevel(logging.DEBUG)`

### download_dataset
This function executes the following actions:
- delete everything from the landing folder.
- builds the command to invoke the kaggle api.
- invokes the API with the usage of the kaggle.json file stored inthe /root/.kaggle folder.
- unzips the archive in the landing folder.
- deletes the .zip archive.

Raises:
- TypeError: the error raises whenever the input types are not correct.

Args:
- active_folder_name(str): folder where to execute all the operations.

### create_spark_session
This function creates a SparkSession to use during the execution of the program. The appName is given as parameter.

Args:
- app_name (str): parameter for the appName.

Returns:
- SparkSession: spark session to use during the execution.

### read_df_from_path
This function reads the file(s) stored in a path and creates a dataframe out of it.

Args:
- spark (SparkSession): spark session in use.
- file_format (str): format of source file(s). The function handles csv and parquet.
- file_path (str): path holding the file(s) to read.
- optional_parameters (dict, optional): optional parameters for the reading configuration. Defaults to {}.

Raises:
- TypeError: the error raises whenever the input types are not correct.
- ValueError: the error raises whenever the file_format is not csv or parquet.

Returns:
- DataFrame: dataframe containing the read data.

### write_df_to_path_as_parquet

This function writes the provided DataFrame as parquet files to the given path. If the data has to be partitioned the function will partition by the keys year, month, day.

Args:
- df (DataFrame): DataFrame containing the data to write.
- file_path (str): path where the data will be written.
- writing_mode (str): writing mode for the writing operation. The function handles append, overwrite, ignore, errorifexists.
- is_partitioned (bool): parameter marking the need to partition data.

Raises:
- TypeError: the error raises whenever the input types are not correct.
- ValueError:  the error raises whenever the file_format is not in the list append, overwrite, ignore, errorifexists.

# Deployment
## Docker 
Follow these steps to deploy the Docker container:
- Insert your information in the kaggle.json file.
- Run the command `docker build -t hema_assignment .`.
- Run the command `docker run -it hema_assignment /bin/bash` to access the docker image shell.
- Run the command `python3 etl_process.py` to execute the pipeline. 

## Databricks
The `databricks.json` file provided shows a mock up of a possible deployment with the jobs API offered by Databricks for AWS, more can be found [here](https://docs.databricks.com/en/archive/dev-tools/cli/jobs-cli.html#create-a-job).