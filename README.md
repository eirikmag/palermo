# Palermo

Palermo provides helper methods meant for use in a development framework of Delta lakehouses in Azure Synapse Notebooks. The functions are built with as few dependencies as possible to allow for copy/paste-portability.


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
* auto_compact (optional): boolean setting for the Delta autoOptimize.autoCompact option. Defaults to True.
* change_data_feed (optional): boolean setting for the Delta delta.enableChangeDataFeed option. Defaults to False.
* partition_by: (optional): a string representation of the column(s) you want to partition your delta table by.

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

CREATE OR REPLACE VIEW silver.fact_transaction
AS 

SELECT
    t.transaction_id
    ,t.transaction_date
    ,t.transaction_type
    ,t.transaction_amount
    ,t.modified_date
    ,p.region_id
FROM bronze.all_transactions t
LEFT JOIN bronze.person p ON t.person_id = p.person_id
```

In the next cell you should trigger the function:
```python
%pyspark 

pal_mergo(
    source_view = "silver.fact_transaction",
    primarykey_columns =  "transaction_id",
    watermark_column = "modified_date",
    destination_table = "gold.fact_transaction",
    merge_schema = False
    )
```

After which you should be able to add a trigger of the notebook.


## Destination Location Generator

The  `destination_location_generator` generates a full storage path in the format of "abfss://medallion@storageaccounturi.dfs.core.windows.net/table_type/table_type_destination_table_name". 
The purpose is to use this for generation of consistant file paths for delta tables in the case of not using managed tables. Currently the function has been built for use in Synapse notebooks and it also only supports azure storage acounts gen 2. 

### Function parameters:
* storageaccounturi: The URI of the azure storage account where we want to store our delta table.
* destination_medallion: a string defining the medallion of the tables (https://docs.databricks.com/lakehouse/medallion.html).
* destination_table_type: a string defining the table type of a table (e.g. 'fact' or 'dim').
* destination_table_name: a string defining the table name (transaction).

### Function returns:
* destination_location: a string that can be used for registering the location path a new spark table. e.g. "".


### Example of use:
The following use:
```python
%pyspark 

destination_location_generator(
    accounturi = "altrohotboys"
    destination_medallion = "gold",
    destination_table_type =  "fact",
    destination_table_name = "transaction"
    )
```
will return:
```python
-->'abfss://gold@altrohotboys.dfs.core.windows.net/fact/fact_transaction/'
```

## Destination Table Generator
The `destination_table_generator` generates the destination table name for a given medallion and table name, all in lower case and in a consistent format. The purpose is to use this for the generation of a consistent naming convention for destination tables.

### Function parameters:
* destination_medallion (str): The name of the medallion where the destination table is located.
* destination_table_type (str): The type of the destination table (e.g. delta, parquet, etc.).
* destination_table_name (str): The name of the destination table.

### Function returns:
destination_table_name (str): The destination table name in the format "medallion.table_type_table_name".

### Example of use:
The following use:
```python
%pyspark

destination_table_generator(
    destination_medallion = "gold",
    destination_table_type =  "fact",
    destination_table_name = "transaction"
    )
```
will return:
```python
-->'gold.fact_transaction'
```

## Hoover
The `hoover` vacuums all delta tables in a Spark database using the VACUUM command. It only works for Delta tables.

### Function parameters:
* spark_db (str): The name of the Spark database to vacuum tables in.
* retention_hours (int): The retention period to use when vacuuming tables, in hours. Must be an integer greater than or equal to 168.

### Function returns:
* None

### Example of use:
The following use:
```python
%pyspark

hoover(
    spark_db = "my_database",
    retention_hours = 168
    )
```

Will vacuum all Delta tables in the "my_database" Spark database with a retention period of 168 hours. If a table is not a Delta table, it will be skipped and a message will be printed to the console. If the retention period provided is less than 168 hours, a ValueError will be raised.



## Dimwit
The `dimwit` function writes or updates a Delta table from a registered view and ensures persistence of surrogate keys over time. This function is intended to be used as part of a data pipeline in a Spark environment. It takes in several inputs and produces a Delta table as its output.

### Function parameters:
* source_view: a string representing the name of the Spark view containing the data to be written to the Delta table.
* unique_keys: a string representing the unique key column(s) to be used to filter and match data in both the source and destination tables, e.g. 'employee_id, date'.
* order_keys_by: a string representing the column to be used to sort and generate row numbers, e.g. 'date'.
* surrogate_key_name: a string representing the name of the surrogate key to be generated for the Delta table.
* destination: a string representing the destination/catalog+table name to store the Delta table, e.g. 'gold.dim_potato'.
* location: a string representing the physical storage location to store the Delta table.
* test_for_duplicates: a boolean indicating whether the function should test for duplicates in the filtered DataFrame before writing/updating the Delta table. Default is False.

### Function behavior:
The function first extracts the column names and data types from the Delta table description of the input view. If the destination Delta table already exists, it reads the maximum surrogate key from the destination and sets incremental_id to begin building keys on. Otherwise, it generates the Delta table dynamically based on input arguments.

Next, it creates a filter condition that excludes rows where any unique key is null, reads data from the source view, and filters by the unique key. If test_for_duplicates is set to True, it checks for duplicates in the filtered DataFrame.

The function then reads data from the destination Delta table and joins it with the filtered updates table on the unique key. It generates an increasing surrogate key, replaces null values in the surrogate key column with generated IDs, and merges the updates into the destination Delta table using the surrogate key. Finally, it returns a string indicating that the data from the source view has been updated into the Delta table.

### Example of use:
In a notebook context, a typical use case would involve you defining a Spark view in one cell:


```sql
%sql

CREATE OR REPLACE VIEW silver.dim_product
AS 

SELECT
    p.product_id
    ,p.product_name
    ,pg.product_category_id
    ,pg.product_category_name
    ,p.region_id
    ,p.modified_date
FROM bronze.product p
JOIN bronze.product_group pg

```
Let's assume the aforementioned views rows are unique by product_id and region_id

In the next cell you should trigger the function like this:
```python
%pyspark 

dimwit(
    source_view = 'silver.dim_product', 
    unique_keys = 'product_id, region_id',
    order_keys_by = 'product_id', 
    surrogate_key_name = 'product_sk',
    destination = 'gold.dim_product',
    location = 'abfss://gold@altrohotboys.dfs.core.windows.net/dim/dim_product/',
    test_for_duplicates = True
    )
```

This will produce a table defined identical to silver.dim_product, but with a column named `product_sk` that is unique.

### Caveats:
If the `test_for_duplicates` argument is set to False, the function will produce a table regardless if the defined `unique_keys` are actually unique or not. If you are unsure if your columns actually produce uniqueness you could test producing the table with the `test_for_duplicates` parameter set to `True` first and set it to `False` if you find the test to costly in a production setting.



## Cata_logger
The cata_logger function retrieves catalog information for all Delta tables in a Spark database using the DESCRIBE HISTORY command. It returns a DataFrame with historical information for each table from the delta transactional log, including operation metrics such as the number of target rows copied, deleted, inserted, updated, execution time, scan time, etc.

### Function parameters:
catalog (str): The name of the Spark catalog to list data from.

### Function behavior:
The function iterates through all tables in the specified Spark catalog and retrieves their history information using the DESCRIBE HISTORY command. Only Delta tables are considered, while views and non-Delta tables are excluded from the iteration.

The resulting DataFrame includes the following columns:

database: The name of the database.
tableName: The name of the table.
Additional columns: Additional columns providing detailed operation metrics for each table, such as the number of target rows copied, deleted, inserted, updated, etc.
Note: This function assumes that the tables in the catalog are Delta tables and have a history that can be described. If non-Delta tables are present or if a table's history is unavailable, the function may break.

### Example of use:
```python
### Call the cata_logger function
df = cata_logger("your_catalog_name")

# Show the resulting DataFrame
df.show()
```

This will display all historical information for all Delta tables in the specified catalog.

Note: Make sure to replace "your_catalog_name" with the actual name of your Spark catalog.