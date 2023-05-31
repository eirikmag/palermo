from delta.tables import *
from pyspark.sql.functions import col, row_number, coalesce, lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window


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



def dimwit (
        source_view: str, 
        unique_keys: str, 
        order_keys_by:str, 
        surrogate_key_name: str, 
        destination: str, 
        location: str,
        test_for_duplicates: bool = False
) -> None:
    """
    dimwit writes or updates a Delta table from a registered view and ensures persistance of surrogate keys over time.

    Parameters:
        source_view (str): The name of the table name to write to.
        unique_keys (str): The name of columns in source_view that should identify uniqueness.
        order_keys_by (str): The name of a column to sort to get to produce row_numbers.
        surrogate_key_name: (str): The name of the surrogate key to generate
        destination: (str): The destination/catalog+table name to store the table as. eg. gold.dim_potato
        location: (str): The "physical storage location" to store the table.

    Returns:
        None.
    """

    # Get column names and data types from delta table description of input view
    table_description = spark.sql(f"DESCRIBE {source_view}")

    # Extract column names and types as lists
    col_names, data_types = table_description.selectExpr("collect_list(col_name)", "collect_list(data_type)").first()

    try:
        max_id = spark.sql(f"SELECT MAX({surrogate_key_name}) FROM {destination}").first()[0]
        if max_id is None:
            max_id = 0
    # If there is no ID. There is no supported table, so we generate one dynamically based on input arguments.
    except AnalysisException:
        # Set incremental_id to begin building keys on
        max_id = 0
        # construct CREATE TABLE statement dynamically based on input view.
        columns = " ,".join([f"{name} {data_type}" for name, data_type in zip(col_names, data_types)])
        initial_table_definition = f'''
          CREATE OR REPLACE TABLE {destination} 
          ({surrogate_key_name} BIGINT not null, {columns})
          USING DELTA
          LOCATION "{location}"
          '''
        spark.sql(initial_table_definition)

    # Create a filter condition that excludes rows where any unique key is null
    unique_cols = [col.strip() for col in unique_keys.split(",")] if unique_keys else None
    filter_condition = col(unique_cols[0]).isNotNull()
    for col_name in unique_cols[1:]:
        filter_condition &= col(col_name).isNotNull()

    # Read data from source view and filter by unique key
    df_updates = spark.read.table(source_view).filter(filter_condition)

  # Check for duplicates in the filtered DataFrame if test_for_duplicates = True
    if test_for_duplicates:
        duplicates = df_updates.groupby(unique_cols).count().filter("count > 1")
        if duplicates.count() > 0:
            return f"ERROR: Duplicates found in {source_view} on columns {unique_cols}"


    # Read data from destination table
    df_destination = spark.read.table(destination)

    # Join destination table and updates table on unique key and get full dimension set in memory
    df_updates = df_destination.alias("destination").join(
        other=df_updates.alias("updates"),
        on=[col("destination." + col_name) == col("updates." + col_name) for col_name in unique_cols],
        how='full_outer'
    ).selectExpr(f"destination.{surrogate_key_name} as {surrogate_key_name}", "updates.*")

    # Define a window specification to partition by null ID values
    w = Window.partitionBy(df_updates[surrogate_key_name].isNull()).orderBy(order_keys_by)

    # Generate an increasing ID
    incremental_id = row_number().over(w) + max_id

    # Replace null values in the ID column with generated IDs
    df_updates = df_updates.withColumn(surrogate_key_name, coalesce(surrogate_key_name, incremental_id))

    # Merge updates into destination using surrogate key
    table_destination = DeltaTable.forName(spark, destination)
    table_destination.alias("destination") \
        .merge(df_updates.alias("updates"),f"updates.{surrogate_key_name} = destination.{surrogate_key_name}") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    return_message = f"Updated data from '{source_view}' into Delta table: '{destination}'"
    return(return_message)


def cata_logger(catalog: str) -> None:
    """
    Retrieves catalog information for all delta tables in a Spark database using the DESCRIBE HISTORY command.

    Parameters:
        catalog (str): The name of the Spark catalog to list data from.

    Raises:
        None.

    Returns:
        DataFrame: A DataFrame containing catalog information for all delta tables in the specified catalog.
                   The DataFrame includes the following columns:
                   - database: The name of the database.
                   - tableName: The name of the table.
                   - Other columns: Additional columns providing detailed operation metrics for each table,
                     such as the number of target rows copied, deleted, inserted, updated, etc.
                     See the code comments for a complete list of the additional columns.

    This function iterates through all tables in the specified Spark catalog and retrieves their history information
    using the `describe history` command. Delta tables are considered, while views are excluded from the iteration.

    The resulting DataFrame includes the catalog, database, and tableName columns as the first three columns,
    followed by additional columns representing operation metrics for each table. The additional columns provide
    detailed information about the vacuum operations performed on each table, such as the number of rows copied,
    deleted, inserted, updated, execution time, scan time, etc. These operation metrics can be useful for monitoring
    and analyzing the performance of the vacuum operations.

    Note: This function assumes that the tables in the catalog are Delta tables and have a history that can be described.
    If non-Delta tables are present or if a table's history is unavailable, an error may occur.
    """
    result_df = None

    # List all tables in the database.
    tables = spark.catalog.listTables(catalog)

    # Iterate through the tables and append all to full dataframe
    for table in tables:
        if table.tableType != "VIEW": # Exclude views from the list to iterate
            df = spark.sql(f"describe history {table.database}.{table.name}")
            df = df.withColumn("database", lit(table.database))
            df = df.withColumn("tableName", lit(table.name))

            if result_df is None:
                result_df = df
            else:
                result_df = result_df.unionAll(df)

    # Reorder columns to place catalog, database, and tableName at the beginning
    result_df = result_df.select("database", "tableName", *result_df.columns)

    # Add additional columns to the result_df DataFrame
    result_df = result_df.withColumn("numTargetRowsCopied", col("operationMetrics.numTargetRowsCopied"))
    result_df = result_df.withColumn("numTargetRowsDeleted", col("operationMetrics.numTargetRowsDeleted"))
    result_df = result_df.withColumn("numTargetFilesAdded", col("operationMetrics.numTargetFilesAdded"))
    result_df = result_df.withColumn("executionTimeMs", col("operationMetrics.executionTimeMs"))
    result_df = result_df.withColumn("numTargetRowsInserted", col("operationMetrics.numTargetRowsInserted"))
    result_df = result_df.withColumn("unmodifiedRewriteTimeMs", col("operationMetrics.unmodifiedRewriteTimeMs"))
    result_df = result_df.withColumn("scanTimeMs", col("operationMetrics.scanTimeMs"))
    result_df = result_df.withColumn("numTargetRowsUpdated", col("operationMetrics.numTargetRowsUpdated"))
    result_df = result_df.withColumn("numOutputRows", col("operationMetrics.numOutputRows"))
    result_df = result_df.withColumn("numTargetChangeFilesAdded", col("operationMetrics.numTargetChangeFilesAdded"))
    result_df = result_df.withColumn("numSourceRows", col("operationMetrics.numSourceRows"))
    result_df = result_df.withColumn("numTargetFilesRemoved", col("operationMetrics.numTargetFilesRemoved"))
    result_df = result_df.withColumn("rewriteTimeMs", col("operationMetrics.rewriteTimeMs"))
    # Add more additional columns as needed

    return result_df