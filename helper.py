import inspect
import logging
import subprocess
from zipfile import ZipFile

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.handlers.clear()
stream_hand = logging.StreamHandler()
stream_hand.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)
logger.addHandler(stream_hand)
logger.setLevel(logging.INFO)


def download_dataset(active_folder_name: str) -> None:
    """
    Summary:
        This function executes the following actions:
        - delete everything from the landing folder
        - builds the command to invoke the kaggle api
        - invokes the API with the usage of the kaggle.json file stored in
        the ~/.kaggle folder
        - unzips the archive in the landing folder
        - deletes the .zip archive

    Raises:
        TypeError: the error raises whenever the input types are not correct

    Args:
        active_folder_name(str): folder where to execute all the operations
    """
    current_function_name = inspect.currentframe().f_code.co_name
    logger.info(msg=f"{current_function_name}: started")
    if not (isinstance(active_folder_name, str)):
        logger.error(
            msg=f"{current_function_name}: Input parameters type are not correct"
        )
        raise TypeError("Input parameters type are not correct")

    subprocess.run(["rm", "-f", f"./{active_folder_name}/*"])

    dataset_name = "rohitsahoo/sales-forecasting"

    # Construct the command as a list of strings
    command = [
        "kaggle",
        "datasets",
        "download",
        "-d",
        dataset_name,
        "-p",
        f"./{active_folder_name}",
    ]

    # Run the command using subprocess
    subprocess.run(command)

    with ZipFile(
        f"./{active_folder_name}/{dataset_name.split('/')[1]}.zip", "r"
    ) as zip_ref:
        zip_ref.extractall("./landing")

    subprocess.run(
        ["rm", "-f", f"./{active_folder_name}/{dataset_name.split('/')[1]}.zip"]
    )
    logger.info(msg=f"{current_function_name}: end")


def create_spark_session(app_name: str) -> SparkSession:
    """
    Summary:
        This function creates a SparkSession to use during the execution of the program.
        The appName is given as parameter.

    Args:
        app_name (str): parameter for the appName

    Returns:
        SparkSession: spark session to use during the execution
    """
    current_function_name = inspect.currentframe().f_code.co_name
    logger.info(msg=f"{current_function_name}: started")
    if not (isinstance(app_name, str)):
        logger.error(
            msg=f"{current_function_name}: Input parameters type are not correct"
        )
        raise TypeError("Input parameters type are not correct")
    logger.info(msg=f"{current_function_name}: end")
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_df_from_path(
    spark: SparkSession, file_format: str, file_path: str, optional_parameters={}
) -> DataFrame:
    """
    Summary:
        This function reads the file(s) stored in a path and creates a dataframe out
        of it.

    Args:
        spark (SparkSession): spark session in use
        file_format (str): format of source file(s). The function handles csv and
        parquet
        file_path (str): path holding the file(s) to read
        optional_parameters (dict, optional): optional parameters for the reading
        configuration. Defaults to {}.

    Raises:
        TypeError: the error raises whenever the input types are not correct
        ValueError: the error raises whenever the file_format is not csv or parquet

    Returns:
        DataFrame: dataframe containing the read data
    """
    current_function_name = inspect.currentframe().f_code.co_name
    logger.info(msg=f"{current_function_name}: started")
    logger.debug(
        msg=f"""{current_function_name}: reading file in path: {file_path} of format: {file_format}"""
    )
    if not (
        isinstance(spark, SparkSession)
        and isinstance(file_format, str)
        and isinstance(file_path, str)
        and isinstance(optional_parameters, dict)
    ):
        logger.error(
            msg=f"{current_function_name}: Input parameters type are not correct"
        )
        raise TypeError("Input parameters type are not correct")

    if file_format == "parquet":
        read_dataframe = spark.read.parquet(file_path)
    elif file_format == "csv":
        if (
            "header" in optional_parameters.keys()
            and "infer_schema" in optional_parameters.keys()
        ):
            read_dataframe = spark.read.csv(
                file_path,
                header=optional_parameters["header"],
                inferSchema=optional_parameters["infer_schema"],
            )
        else:
            read_dataframe = spark.read.csv(file_path)
    else:
        logger.error(msg=f"{current_function_name}: provided the wrong file format")

    logger.info(msg=f"{current_function_name}: ended")
    return read_dataframe


def write_df_to_path_as_parquet(
    df: DataFrame, file_path: str, writing_mode: str, is_partitioned: bool
) -> None:
    """
    Summary:
        This function writes the provided DataFrame as parquet files to the given path.
        If the data has to be partitioned the function will partition by the keys
        year, month, day.

    Args:
        df (DataFrame): DataFrame containing the data to write
        file_path (str): path where the data will be written
        writing_mode (str): writing mode for the writing operation. The function handles
        append, overwrite, ignore, errorifexists
        is_partitioned (bool): parameter marking the need to partition data

    Raises:
        TypeError: the error raises whenever the input types are not correct
        ValueError:  the error raises whenever the file_format is not in the list
        append, overwrite, ignore, errorifexists
    """
    current_function_name = inspect.currentframe().f_code.co_name
    logger.info(msg=f"{current_function_name}: started")
    logger.debug(
        msg=f"""{current_function_name}: writing dataframe in path: {file_path} with mode: {writing_mode} {'and partitioning the data' if is_partitioned else ''}"""
    )
    if not (
        isinstance(df, DataFrame)
        and isinstance(file_path, str)
        and isinstance(writing_mode, str)
        and isinstance(is_partitioned, bool)
    ):
        logger.error(
            msg=f"{current_function_name}: Input parameters type are not correct"
        )
        raise TypeError("Input parameters type are not correct")

    if writing_mode not in ["append", "overwrite", "ignore", "errorifexists"]:
        logger.error(
            f"""{current_function_name}: mode provided ({writing_mode}) is not available."""
        )
        raise ValueError(f"Mode provided ({writing_mode}) is not available.")
    if is_partitioned and all(
        value in df.schema.names for value in ["year", "month", "day"]
    ):
        df.write.partitionBy("year", "month", "day").save(
            format="parquet", mode=writing_mode, path=file_path
        )
    else:
        df.write.save(format="parquet", mode=writing_mode, path=file_path)
    logger.info(msg=f"{current_function_name}: ended")
