from metadata_utils import *


# Process parameters
workflow_parameters = get_workflow_parameters()

datasource_name = workflow_parameters["--datasource_name"]
catalog_name = workflow_parameters.get("--catalog_name")

execution_set_name = (workflow_parameters.get("--execution_set_name")
                      if workflow_parameters.get("--execution_set_name") != '' else None)



from spark_utils import *
import json


if __name__ == "__main__":
    #execution starts from here
    workflow_parameters = get_workflow_parameters()

    datasource_name = workflow_parameters["--datasource_name"]
    catalog_name = workflow_parameters.get("--catalog_name")
    schema_name = workflow_parameters.get("--schema_name")
    table_name = workflow_parameters.get("--table_name")


    #Create spark session and logger objects
    spark = get_spark_session(app_name=f"{datasource_name}_landing_to_bronze",
                              config_list=[("spark.databricks.delta.optimizeWrite.enabled", "true"),
                                           ("spark.databricks.delta.autoCompact.enabled", "true")])
    logger = get_logger("landing_to_bronze.py")

    logger.info("Spark session created")
    #logger.info(f"Parameters: {parameters}")

    #Create requied schema in catalog
    create_db(spark, catalog_name, schema_name)
    logger.info(f"Schema {schema_name} created in catalog {catalog_name}")

    #Generate required table schema
    table_schema=get_schema_str(spark)
    logger.info(f"Table schema: {table_schema}")

    #create empty table in catalog
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    table_stmt = create_table(full_table_name,table_schema)

    #Run create table stmt
    run_statement(spark, table_stmt)
    logger.info(f"Table {full_table_name} created")

    #Reading input data
    read_df=read_stream(spark)

    #writing dataframe into table
    write_stream(spark,read_df,full_table_name)
    logger.info(f"Data written to table {full_table_name}")








