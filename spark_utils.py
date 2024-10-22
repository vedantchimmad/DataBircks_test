from pyspark.sql import SparkSession
import logging
import json
import sys


def get_spark_session(app_name: str, config_list: list = None) -> SparkSession:
    """
    Gets and configures the SparkSession, or creates one if
        it doesn't exist yet.

    Parameters:
        app_name: Spark application name
        config_list: a list of tuples containing the key and value
            pairs of each Spark configuration setting

    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder.appName(app_name)

    if config_list is not None:
        for config in config_list:
            builder = builder.config(key=config[0], value=config[1])

    spark = builder.getOrCreate()

    return spark


def get_logger(block_name: str):
    """
    Returns a logger object that writes to the driver log.

    Parameters:
        block_name: the identifier of the running block

    Returns:
        A logger object
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(module)s[%(filename)s:%(lineno)d] %(message)s',
        datefmt='%y/%d/%m %H:%M:%S',
        level=logging.INFO)
    return logging.getLogger(block_name)

def get_schema_str(spark_session: SparkSession) -> str:
    """
    Load the schema string from the schema json file of a specific control table.

    Parameters:
        spark_session: active SparkSession
        base_location: workflow_config folder location
        file_name: schema json file name

    Returns:
    DDL schema string of the control table.
    """
    schema_json = json.loads(spark_session.read.text(
        "dbfs:/FileStore/source_system/employee/schema/employee.json", wholetext=True
    ).collect()[0][0])["fields"]
    schema_str = ", ".join([f"{fld['name']} {fld['type']}" for fld in schema_json])

    return schema_str
def create_db(spark: SparkSession, catalog_name: str, db_name: str) -> None:
    """
    Creates the table in the metastore at the specified location, in the specified
    database.

    Parameters:
        spark: the active SparkSession
        catalog_name: the name of the target catalog
        db_name: the database name to be created
    """
    create_stmt = f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{db_name}"
    spark.sql(create_stmt)

def create_table(full_table_name: str, schema_str: str) -> str:
    """
    Creates a DDL statement for the table, that will also partition it by the source name.

    Parameters:
        full_table_name: fully qualified table name with catalog and database
        schema_str: DDL schema string of the table

    Returns:
    DDL create statement.
    """
    create_stmt = (
        f"CREATE TABLE IF NOT EXISTS {full_table_name} "
        f"({schema_str}) USING DELTA")

    return create_stmt


def run_statement(spark_session: SparkSession, stmt: str) -> None:
    """
    Runs the statement and prints it to the console.

    Parameters:
        spark_session: active SparkSession
        stmt: statement to run
    """
    #print(f"Running statement: \n{stmt}")
    spark_session.sql(stmt)

def read_stream(spark_session: SparkSession):
    read_df=spark_session.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("separator", ",") \
        .schema(get_schema_str(spark_session)).load("dbfs:/FileStore/source_system/employee/data/")
    return read_df

def write_stream(spark_session: SparkSession, streaming_df, table_name):
    streaming_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", "/dbfs/FileStore/source_system/employee/checkpoint") \
        .table(table_name) \
        .start()

def get_workflow_parameters() -> dict:
    """
    Processes the received workflow parameters and returns them as a dict.
    """
    params_list = sys.argv[1:]
    params = {}

    for i in range(len(params_list) // 2):
        params[params_list[i * 2]] = params_list[(i * 2) + 1]

    return params