## Create Dataframe using SparkSession

from pyspark.sql import SparkSession <br/>
spark = SparkSession.builder.appName('Basics').getOrCreate() <br/>
df  = spark.read.json('/FileStore/tables/people.json') <br/>
df.show() <br/>

+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+

df.printSchema() <br/>

root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)


df.columns <br/>

['age', 'name'] <br/>

df.describe() <br/>

DataFrame[summary: string, age: string, name: string] <br/>

df.describe().show() <br/>

+-------+------------------+-------+
|summary|               age|   name|
+-------+------------------+-------+
|  count|                 2|      3|
|   mean|              24.5|   null|
| stddev|7.7781745930520225|   null|
|    min|                19|   Andy|
|    max|                30|Michael|
+-------+------------------+-------+

### If you need to infer schema correctly by specifying your expected data types then 

from pyspark.sql.types import StructField,StringType,IntegerType,StructType <br/>

data_schema = [StructField('age',IntegerType(),True), <br/>
              StructField('name',StringType(),True)] <br/>
              
final_struc = StructType(fields=data_schema) <br/>

df = spark.read.json('/FileStore/tables/people.json',schema=final_struc) <br/>

df.show() <br/>

+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+

df.printSchema() <br/>

root
 |-- age: integer (nullable = true)
 |-- name: string (nullable = true)
 
 


