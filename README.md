# Palermo

Palermo provides a helper method to make it easier to approach creation of and upserts to spark delta tables.

![palermo](https://github.com/eirikmag/palermo/blob/main/images/palermo.jfif)


## Palermo merge

The `pal_mergo` function handles the creation of a a spark table and the upserts to said table based on very few inputs. The solution is very general and only handles managed tables.
The functions purpose is partially to force consistancy in how to handle Notebook-based data pipelines. It could be used as a helper method as part of a framework.

The function has only been tested in Azure Synapse Notebooks using Apache Spark version 3.3.

### Function parameters:

* source_view: a string representing the name of the pre-defined Spark view containing the data to be written to the Delta table.
* primarykey_columns: a string representing the primary key column(s) to be used as join clauses for merge, e.g. 'RegionID, RowID'.
* watermark_column: a string representing the column to be used as a watermark to identify the new data to be merged with the existing Delta table.
* destination_table: a string representing the name of the desired Delta table. 
* destination_location (optional): a string representing the storage location of the desired Delta table.
* merge_schema (optional): boolean setting for the Delta mergeSchema option. Defaults to True.
* optimize_write (optional): boolean setting for the Delta autoOptimize.optimizeWrite option. Defaults to True.
* auto_compact (optional): boolean setting for the Delta autoOptimize.autoCompac option. Defaults to True.

### Function returns:

* return_message: a string message describing the action performed by the function, i.e. whether a new Delta table has been created or existing Delta table has been updated.


### The function works as follows:

- It first tries to get the 'MAX' value of the watermark column from the destination Delta table using an SQL query.
- If there is no watermark (i.e. the query returns None), the function writes the data from the source view to the Delta table using the 'overwrite' mode.
- If there is a watermark, the function filters the new data from the source view based on the watermark column and joins it with the existing Delta table using the primary key column(s) to merge the data. 
- It then updates the existing Delta table with the new data and inserts the rows that do not already exist in the Delta table.

### Example of use:
In a notebook context, a typical use case would involve you defining a Spark view in one cell:


```sql
%sql

CREATE OR REPLACE VIEW silver.fact_transaction_pm
AS 

SELECT
    t.transactionid
    ,t.transactiondate
    ,t.transactiontype
    ,t.transactionamount
    ,t.modifieddate
    ,p.regionid
FROM bronze.alltransactions t
LEFT JOIN bronze.person p ON t.personid = p.personid
```

In the next cell you should trigger the function:
```python
%pyspark 

pal_mergo(
    source_view = "silver.fact_transaction_pm",
    primarykey_columns =  "transactionid",
    watermark_column = "modifieddate",
    destination_table = "gold.fact_transaction"
    )
```

After which you should be able to add a trigger of the notebook.