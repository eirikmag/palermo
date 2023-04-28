from delta.tables import *
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException



def pal_mergo (
        source_view: str,
        primarykey_columns: str,
        watermark_column: str,
        destination_table: str,
        destination_location: str = None,
        merge_schema: bool = True,
        optimize_write: bool = True,
        auto_compact: bool = True,        
        partition_by: str = None,
        change_data_feed: bool = False
) -> None:
    """
    Function for incrementally writing to a delta table from a pre-defined source spark view. Does not depend on table being created previously

    Parameters:
        source_view (str): The name of the Spark view to be used as a source.
        primarykey_columns (str): The primary key column(s) to be used as join clauses for merge (e.g. "RegionID, RowID").
        watermark_column (str): The column used to determine which records to update.
        destination_table (str): The name of the Spark Delta Lake table to create or upsert to.
        destination_location (str, optional): The storage location of the Delta Lake table.
        merge_schema (bool, optional): A boolean value to set merge schema to true/false.
        optimize_write (bool, optional): A boolean value to set optimize write to true/false.
        auto_compact (bool, optional): A boolean value to set auto-compact to true/false.
        partition_by (string, optional): A string value to specify on which columns to partition delta tables.
        change_data_feed (bool, optional): A boolean value to set auto-compact to true/false.

    Returns:
        None: This function returns None.

    Raises:
        ValueError: Raises ValueError if any of the input parameters are empty or not of type str.
        AnalysisException: If the maximum watermark is not found in the Delta table, a new table is created with the data from the source view.

    """
    # Simple parameter validation to check if all inputs have value
    if not all(isinstance(p, str) and len(p) > 0 for p in [source_view, primarykey_columns, watermark_column, destination_table, destination_location]):
        raise ValueError("All parameters must be non-empty strings")


    # Initially we (try to) get the 'MAX' value of our watermark written to the table.
    try:
        max_watermark = spark.sql(f"SELECT MAX({watermark_column}) FROM {destination_table}").first()[0]
    
    # If there is no watermark. There is no supported table, so we (over)write one with data from source.
    except AnalysisException:
        max_watermark = None
        dfupdates = spark.read.table(source_view)
        writer = dfupdates.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", merge_schema) \
            .option("delta.autoOptimize.optimizeWrite", optimize_write) \
            .option("delta.autoOptimize.autoCompact", auto_compact) \
            .option("delta.enableChangeDataFeed", change_data_feed)
        if destination_location is not None:
            writer = writer.option("path", destination_location)
        if partition_by is not None:
            writer = writer.partitionBy(partition_by)
        writer.saveAsTable(destination_table)
        return_message = f"Creating new Delta table: '{destination_table}' from: '{source_view}'"
    except:
        return_message = f"Something went wrong trying to retrieve the maximum of {watermark_column} from {destination_table}"
        return(return_message)
    

    # If watermark exists, the table exists and we should merge into this.
    if max_watermark:
        dfupdates = spark.read.table(source_view).filter(col(watermark_column) >= max_watermark)

        #gen join_clause
        pk_columns = [col.strip() for col in primarykey_columns.split(",")] if primarykey_columns else None
        join_clause = " AND ".join([f"destination.{col} = updates.{col}" for col in pk_columns]) if pk_columns else None

        dfdestination = DeltaTable.forName(spark, destination_table)
        dfdestination.alias("destination") \
            .merge(dfupdates.alias("updates"),join_clause) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        return_message = f"Merged data from '{source_view}' into Delta table: '{destination_table}'"
    return(return_message)


def destination_location_generator(
        storageaccounturi: str,
        destination_medallion: str,
        destination_table_type: str,
        destination_table_name: str,
        storage_type: str = "azure"
        ) -> str:
    """
    Generates the storage location for a given medallion and table name in Azure Data Lake Storage Gen2 or other similar services.

    Parameters:
    storageaccounturi (str): The URI of the storage account where the destination location is located.
    destination_medallion (str): The name of the medallion where the destination location is located.
    destination_table_type (str): The type of the destination table (e.g. delta, parquet, etc.).
    destination_table_name (str): The name of the destination table.
    storage_type (str): The type of the storage service, defaults to "azure".

    Returns:
    str: The destination location string in the format "abfss://{medallion}@{storage_account}.dfs.core.windows.net/{table_type}/{table_name}/".
    """

    if storage_type == "azure":
        # Generate the destination location string for Azure Data Lake Storage Gen2
        destination_location = f"abfss://{destination_medallion}@{storageaccounturi}.dfs.core.windows.net/{destination_table_type}/{destination_table_type}_{destination_table_name}/".lower()

    # Return the destination location string
    return destination_location



def destination_table_generator(
        destination_medallion: str,
        destination_table_type: str,
        destination_table_name: str
        ) -> str :
    """
    Generates the destination table name for a given medallion and table name all in lower case and in a consistant format.

    Parameters:
    destination_medallion (str): The name of the medallion where the destination table is located.
    destination_table_type (str): The type of the destination table (e.g. delta, parquet, etc.).
    destination_table_name (str): The name of the destination table.

    Returns:
    str: The destination table name in the format "{medallion}_lake.{table_type}_{table_name}".
    """

    # Generate the destination table name
    destination_table_name = f"{destination_medallion}.{destination_table_type}_{destination_table_name}".lower()

    # Return the destination table name
    return destination_table_name


def hoover(
    spark_db: str,
    retention_hours: int
    ) -> None :
    """
    Vacuums all delta tables in a Spark database using the VACUUM command.

    Parameters:
        spark_db (str): The name of the Spark database to vacuum tables in.
        vacuum_retention (int): The retention period to use when vacuuming tables, in hours. 
            Must be a string representation of an integer.

    Raises:
        ValueError: If the `retention_hours` parameter is less than 168.

    Returns:
        None.
    """

    if retention_hours < 168:
        raise ValueError("The retention_hours parameter must be at least 168 hours (1 week)")

    # List all tables in the database.
    tables = spark.catalog.listTables(spark_db)

    # Iterate through the tables and vacuum each one.
    for table in tables:
        if table.tableType != "VIEW": # Exclude views from list to Vacuum
            print(f"Vacuuming table '{table.database}.{table.name}'...")
            deltaTable = DeltaTable.forName(spark, f"{table.database}.{table.name}")
            deltaTable.vacuum(retentionHours = retention_hours)
