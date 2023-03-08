# palermo

palermo provides a helper method to make it easier to approach handling creation of spark delta tables and upserts to said tables.

![palermo](https://github.com/eirikmag/palermo/blob/main/images/palermo.jfif)


## Palermo merge

The `pal_mergo` function handles the creation of a a spark table and the upserts to said table based in a very general way and with only one function. 
The functions purpose is partially to force consistancy in how to handle Notebook-based data pipelines.


```

source_view: a spark view that defines the table you want to create. Must be deployed to spark prior to call of palermo merge.
primarykey_columns: defines one or several columns that defines the row-level uniqueness of the table.
watermark_column: your view MUST have a column that represents updated fields in a sortable manner.
destination_table: the name of the output table 


```

## Example of use

![palermo](https://github.com/eirikmag/palermo/blob/main/images/notebook_example.jpg)