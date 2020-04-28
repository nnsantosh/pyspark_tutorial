## Create Dataframe using SparkSession

from pyspark.sql import SparkSession <br/>
spark = SparkSession.builder.appName('Basics').getOrCreate() <br/>
df  = spark.read.json('/FileStore/tables/people.json') <br/>
df.show() <br/>

+----+-------+ <br/>
| age|   name| <br/>
+----+-------+ <br/>
|null|Michael| <br/>
|  30|   Andy| <br/>
|  19| Justin| <br/>
+----+-------+ <br/>

df.printSchema() <br/>

root <br/>
 |-- age: long (nullable = true) <br/>
 |-- name: string (nullable = true) <br/>


df.columns <br/>

['age', 'name'] <br/>

df.describe() <br/>

DataFrame[summary: string, age: string, name: string] <br/>

df.describe().show() <br/>

+-------+------------------+-------+ <br/>
|summary|               age|   name| <br/>
+-------+------------------+-------+ <br/>
|  count|                 2|      3| <br/>
|   mean|              24.5|   null| <br/>
| stddev|7.7781745930520225|   null| <br/>
|    min|                19|   Andy| <br/>
|    max|                30|Michael| <br/>
+-------+------------------+-------+<br/>

### If you need to infer schema correctly by specifying your expected data types then 

from pyspark.sql.types import StructField,StringType,IntegerType,StructType <br/>

data_schema = [StructField('age',IntegerType(),True), <br/>
              StructField('name',StringType(),True)] <br/>
              
final_struc = StructType(fields=data_schema) <br/>

df = spark.read.json('/FileStore/tables/people.json',schema=final_struc) <br/>

df.show() <br/>

+----+-------+ <br/>
| age|   name| <br/>
+----+-------+ <br/>
|null|Michael| <br/>
|  30|   Andy| <br/>
|  19| Justin| <br/>
+----+-------+ <br/>

df.printSchema() <br/>

root <br/>
 |-- age: integer (nullable = true) <br/>
 |-- name: string (nullable = true) <br/>
 
 
### To derive single column as dataframe use select

df.select('age').show() <br/>

+----+ <br/>
| age| <br/>
+----+ <br/>
|null| <br/>
|  30| <br/>
|  19| <br/>
+----+ <br/>

### To grab first two rows of dataframe use head

df.head(2) <br/>

[Row(age=None, name='Michael'), Row(age=30, name='Andy')] <br/>

NOTE: Head does not return dataframe but List of Row objects <br/>

### To append new column to existing dataframe use withColumn

df.withColumn('double_age',df['age']*2) <br/>

df.withColumn('double_age',df['age']*2).show() <br/>

+----+-------+----------+ <br/>
| age|   name|double_age| <br/>
+----+-------+----------+ <br/>
|null|Michael|      null| <br/>
|  30|   Andy|        60| <br/>
|  19| Justin|        38| <br/>
+----+-------+----------+ <br/>

