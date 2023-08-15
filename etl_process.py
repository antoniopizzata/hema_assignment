import os
import sys
from datetime import datetime

from pyspark.sql.functions import (
    col,
    count,
    datediff,
    format_string,
    input_file_name,
    lit,
    split,
    to_date,
    when,
)

from helper import (
    create_spark_session,
    download_dataset,
    logger,
    read_df_from_path,
    write_df_to_path_as_parquet,
)

dataplatform_layers = {
    "landing": "landing",
    "bronze": "bronze",
    "silver": "silver",
    "golden": "golden",
}
logger.info(msg="Ingestion of source data")

download_dataset(dataplatform_layers["landing"])

logger.info(msg="Ingestion of source data end")

logger.debug(
    msg="""
            *******************
                SECTION ENDED
            *******************"""
)

spark = create_spark_session("ETL Process")

csv_file_path = f"./{dataplatform_layers['landing']}/train.csv"

logger.info(
    msg=f"Ingestion of source data to the {dataplatform_layers['bronze']} layer"
)

try:
    source_df = read_df_from_path(
        spark=spark,
        file_format="csv",
        file_path=csv_file_path,
        optional_parameters={"header": True, "infer_schema": True},
    )
except Exception as e:
    sys.exit(f"Terminating execution for reason: {e}")

logger.debug(
    msg=f"""Created source daframe from ingestion. Record count: {source_df.count()} with schema: {source_df.schema}"""
)

split_order_date = split(source_df["Order Date"], f"/")

source_df = (
    source_df.withColumn("file_path", input_file_name())
    .withColumn("execution_datetime", lit(datetime.now()))
    .withColumn("year", lit(split_order_date.getItem(2)))
    .withColumn("month", lit(split_order_date.getItem(1)))
    .withColumn("day", lit(split_order_date.getItem(0)))
)

logger.debug(msg=f"Writing source data in {dataplatform_layers['bronze']} layer")
try:
    write_df_to_path_as_parquet(
        df=source_df,
        file_path=f'./{dataplatform_layers["bronze"]}/sales_dataset',
        writing_mode="overwrite",
        is_partitioned=True,
    )
except Exception as e:
    sys.exit(f"Terminating execution for reason: {e}")
logger.debug(msg=f"Writing source data in {dataplatform_layers['bronze']} layer end")

del source_df

logger.info(
    msg=f"Ingestion of source data to the {dataplatform_layers['bronze']} layer end"
)

logger.debug(
    msg="""
            *******************
                SECTION ENDED
            *******************"""
)

logger.info(
    msg=f"Ingestion and transformation of data to the {dataplatform_layers['silver']} layer"
)

logger.debug(msg=f"Reading data from {dataplatform_layers['bronze']} layer")
try:
    bronze_df = read_df_from_path(
        spark=spark,
        file_format="parquet",
        file_path=f'./{dataplatform_layers["bronze"]}/sales_dataset',
    )
except Exception as e:
    sys.exit(f"Terminating execution for reason: {e}")

logger.debug(
    msg=f"""Read data from {dataplatform_layers['bronze']} layer. Record count: {bronze_df.count()} with schema: {bronze_df.schema}"""
)


expression = [
    col(column).alias(column.lower().replace(" ", "_").replace("-", "_"))
    for column in bronze_df.columns
]
bronze_df = bronze_df.dropDuplicates()
bronze_df = bronze_df.select(*expression)
bronze_df = (
    bronze_df.withColumn("file_path", input_file_name())
    .withColumn("execution_datetime", lit(datetime.now()))
    .withColumn("order_date", to_date(col("order_date"), "dd/MM/yyyy"))
    .withColumn("ship_date", to_date(col("ship_date"), "dd/MM/yyyy"))
    .withColumn("sales", col("sales").cast("double"))
    .withColumn("month", format_string("%02d", col("month")))
    .withColumn("day", format_string("%02d", col("day")))
)

logger.debug(msg=f"Terminated column transformation. New schema: {bronze_df.schema}")


logger.debug(
    msg=f"""Writing transformed {dataplatform_layers['bronze']} data in layer {dataplatform_layers['silver']}"""
)
try:
    write_df_to_path_as_parquet(
        df=bronze_df,
        file_path=f'./{dataplatform_layers["silver"]}/sales_dataset',
        writing_mode="overwrite",
        is_partitioned=True,
    )
except Exception as e:
    sys.exit(f"Terminating execution for reason: {e}")

logger.debug(
    msg=f"""Writing transformed {dataplatform_layers['bronze']} data in layer {dataplatform_layers['silver']} end"""
)

del bronze_df

logger.info(
    msg=f"Ingestion and transformation of data to the {dataplatform_layers['silver']} layer end"
)

logger.debug(
    msg="""
            *******************
                SECTION ENDED
            *******************"""
)

logger.info(
    msg=f"Ingestion and transformation of data to the {dataplatform_layers['golden']} layer"
)
try:
    silver_df = read_df_from_path(
        spark=spark,
        file_format="parquet",
        file_path=f'./{dataplatform_layers["silver"]}/sales_dataset',
    )
except Exception as e:
    sys.exit(f"Terminating execution for reason: {e}")
logger.debug(
    msg=f"""Read data from {dataplatform_layers['silver']} layer. Record count: {silver_df.count()} with schema: {silver_df.schema}"""
)

silver_df = silver_df.withColumn("file_path", input_file_name())

df_sales_new = silver_df.select(
    "order_id",
    "order_date",
    "ship_date",
    "ship_mode",
    "city",
    "year",
    "month",
    "day",
).dropDuplicates()
df_sales_new = df_sales_new.withColumnRenamed(
    "ship_date", "shipment_date"
).withColumnRenamed("ship_mode", "shipment_mode")

try:
    logger.debug(msg=f"Reading sales data from {dataplatform_layers['golden']} layer")
    df_sales_old = read_df_from_path(
        spark=spark,
        file_format="parquet",
        file_path=f'./{dataplatform_layers["golden"]}/sales',
    )
    logger.debug(
        msg=f"""Read sales data from {dataplatform_layers['golden']} layer. Record count: {df_sales_old.count()} with schema: {df_sales_old.schema}"""
    )

    df_sales_old = df_sales_old.select(
        "order_id",
        "order_date",
        "shipment_date",
        "shipment_mode",
        "city",
        "year",
        "month",
        "day",
    )
except TypeError as e:
    sys.exit(f"Terminating execution for reason: {e}")
except ValueError as e:
    sys.exit(f"Terminating execution for reason: {e}")
except Exception as e:
    logger.debug(
        msg=f"""Sales data is missing from {dataplatform_layers['golden']} layer, creating an empty dataframe"""
    )
    df_sales_old = spark.createDataFrame(
        spark.sparkContext.emptyRDD(), df_sales_new.schema
    )

logger.info(msg=f"Calculating sales data delta")
df_sales_delta = df_sales_new.exceptAll(df_sales_old)

if df_sales_delta.count() == 0:
    logger.info(msg=f"Sales data delta is empty therefore dataset will not be updated")
else:
    logger.info(msg=f"Sales data delta is not empty. Appending data to the dataset")

    df_sales_delta = (
        df_sales_delta.withColumn(
            "path_file",
            lit(f"{os.getcwd()}/{dataplatform_layers['silver']}/sales_dataset"),
        )
        .withColumn("execution_datetime", lit(datetime.now()))
        .withColumnRenamed("ship_date", "shipment_date")
        .withColumnRenamed("ship_mode", "shipment_mode")
        .withColumn("month", format_string("%02d", col("month")))
        .withColumn("day", format_string("%02d", col("day")))
    )

    df_sales_delta = df_sales_delta.select(
        "order_id",
        "order_date",
        "shipment_date",
        "shipment_mode",
        "city",
        "path_file",
        "execution_datetime",
        "year",
        "month",
        "day",
    )

    logger.debug(
        msg=f"Terminated column transformation. New schema: {df_sales_delta.schema}"
    )

    logger.debug(
        msg=f"""Writing sales delta data in {dataplatform_layers["golden"]}/sales"""
    )
    try:
        write_df_to_path_as_parquet(
            df=df_sales_delta,
            file_path=f'./{dataplatform_layers["golden"]}/sales',
            writing_mode="append",
            is_partitioned=True,
        )
    except Exception as e:
        sys.exit(f"Terminating execution for reason: {e}")
    logger.debug(
        msg=f"""Writing sales delta data in 
                 {dataplatform_layers["golden"]}/sales end"""
    )

logger.info(msg=f"Calculating sales data delta end")


logger.info(msg=f"Calculating customer data")


df_customer = silver_df.withColumn(
    "days_since_today", datediff(lit(datetime.now().date()), col("order_date"))
)
conditions_to_split = [
    (col("days_since_today") <= 5, "quantity_of_orders_last_5_days"),
    (col("days_since_today") <= 15, "quantity_of_orders_last_15_days"),
    (col("days_since_today") <= 30, "quantity_of_orders_last_30_days"),
]


df_customer_grouped = df_customer.groupBy(
    df_customer["customer_id"],
    df_customer["customer_name"],
    df_customer["segment"],
    df_customer["country"],
).agg(
    *[
        count(when(condition, True)).alias(column_name)
        for condition, column_name in conditions_to_split
    ],
    count("*").alias("total_quantity_of_orders"),
)


splitted_names = split(df_customer_grouped["customer_name"], f" ")
df_customer_grouped = (
    df_customer_grouped.withColumn("execution_datetime", lit(datetime.now()))
    .withColumn(
        "file_path", lit(f"{os.getcwd()}/{dataplatform_layers['silver']}/sales_dataset")
    )
    .withColumn("customer_first_name", lit(splitted_names.getItem(0)))
    .withColumn("customer_last_name", lit(splitted_names.getItem(1)))
    .withColumnRenamed("segment", "customer_segment")
)

df_customer_grouped = df_customer_grouped.select(
    "customer_id",
    "customer_first_name",
    "customer_last_name",
    "customer_segment",
    "country",
    "quantity_of_orders_last_5_days",
    "quantity_of_orders_last_15_days",
    "quantity_of_orders_last_30_days",
    "total_quantity_of_orders",
    "file_path",
    "execution_datetime",
)

logger.debug(
    msg=f"Terminated column transformation. New schema: {df_customer_grouped.schema}"
)


logger.debug(
    msg=f"""Writing customer data in {dataplatform_layers["golden"]}/customer"""
)

try:
    write_df_to_path_as_parquet(
        df=df_customer_grouped,
        file_path=f'./{dataplatform_layers["golden"]}/customer',
        writing_mode="overwrite",
        is_partitioned=False,
    )
except Exception as e:
    sys.exit(f"Terminating execution for reason: {e}")

logger.debug(
    msg=f"""Writing customer data in {dataplatform_layers["golden"]}/customer end"""
)

logger.info(msg=f"Calculating customer data end")

logger.info(
    msg=f"Ingestion and transformation of data to the {dataplatform_layers['golden']} layer end"
)

spark.stop()
