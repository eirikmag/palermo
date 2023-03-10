from delta.tables import *
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException



def pal_mergo (
        source_view: str,
        primarykey_columns: str,
        watermark_column: str,
        destination_table: str,
        destination_location: str,
        merge_schema: bool = True,
        optimize_write: bool = True,
        auto_compact: bool = True,
) -> None:
    """
    Function for incrementally writing to a delta table from a pre-defined source spark view. Does not depend on table being created previously

    Parameters:
    destination_table: name of the Spark Delta Lake table to create or upsert to.
    primarykey_columns: string with (list of) primary key column(s) to be used as join clauses for merge (e.g. 'RegionID, RowID')

    Returns:
    string: generated join clause for use in a Spark MERGE operation
    """
    # Simple parameter validation to check if all inputs have value
    if not all(isinstance(p, str) and len(p) > 0 for p in [source_view, primarykey_columns, watermark_column, destination_table]):
        raise ValueError("All parameters must be non-empty strings")


    # Initially we (try to) get the 'MAX' value of our watermark written to the table.
    try:
        max_watermark = spark.sql(f"SELECT MAX({watermark_column}) FROM {destination_table}").first()[0]
    
    # If there is no watermark. There is no supported table, so we (over)write one with data from source.
    except AnalysisException:
        max_watermark = None
        dfupdates = spark.read.table(source_view)
        dfupdates.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", merge_schema) \
            .option("delta.autoOptimize.optimizeWrite", optimize_write) \
            .option("delta.autoOptimize.autoCompact", auto_compact) \
            .option("path", destination_location) \
            .saveAsTable(destination_table)
        return_message = f"Creating new Delta table: '{destination_table}' from: '{source_view}'"
    except:
        return_message = f"Something went wrong trying to retrieve the maximum of {watermark_column} from {destination_table}"
        return(return_message)
    

    # If watermark exists, the table exists and we should merge into this.
    if max_watermark:
        dfupdates = spark.read.table(source_view).filter(col(watermark_column) >= max_watermark)

        #gen join_clause
        pk_columns = [col.strip() for col in primarykey_columns.split(',')] if primarykey_columns else None
        join_clause = " AND ".join([f"destination.{col} = updates.{col}" for col in pk_columns]) if pk_columns else None

        dfdestination = DeltaTable.forName(spark, destination_table)
        dfdestination.alias("destination") \
            .merge(dfupdates.alias("updates"),join_clause) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        return_message = f"Merged data from '{source_view}' into Delta table: '{destination_table}'"
    return(return_message)