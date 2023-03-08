from delta.tables import *
from pyspark.sql.functions import col



def pal_mergo (source_view, primarykey_columns, watermark_column, destination_table) :
    """
    Function for incrementally writing to a delta table from a pre-defined source spark view

    Parameters:
    destination_table: name of the Spark Delta Lake table to create or upsert to.
    primarykey_columns: string with (list of) primary key column(s) to be used as join clauses for merge (e.g. 'RegionID, RowID')

    Returns:
    string: generated join clause for use in a Spark MERGE operation
    """

    # Initially we (try to) get the 'MAX' value of our watermark written to the table.
    try:
        max_watermark = spark.sql(f"SELECT MAX({watermark_column}) FROM {destination_table}").first()[0]
    except:
        max_watermark = None

    # If there is no watermark. There is no supported table, so we (over)write one.
    if max_watermark is None:
        dfupdates = spark.table(source_view)
        dfupdates.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", True) \
            .saveAsTable(f"{destination_table}")
        return_message = f"Creating new Delta table: '{destination_table}' from: '{source_view}'"
    # If watermark exists, the table exists and we should merge into this.
    elif max_watermark:
        dfupdates = spark.table(source_view).filter(col(watermark_column) >= max_watermark)

        #gen join_clause
        pkcols = [col.strip() for col in primarykey_columns.split(',')] if primarykey_columns else None
        join_clause = " AND ".join([f"destination.{col} = updates.{col}" for col in pkcols]) if pkcols else None

        dfdestination = DeltaTable.forName(spark, destination_table)
        dfdestination.alias("destination") \
            .merge(dfupdates.alias("updates"),join_clause) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        return_message = f"Merged data from '{source_view}' into Delta table: '{destination_table}'"
    return(return_message)