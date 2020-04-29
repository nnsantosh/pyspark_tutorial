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

### To register dataframe as table and run sql queries

df.createOrReplaceTempView('people') <br/>

newDf = spark.sql("select * from people") <br/>

newDf.show() <br/>

results = spark.sql("select * from people where age=30") <br/>

results.show() <br/>

+---+----+ <br/>
|age|name| <br/>
+---+----+ <br/>
| 30|Andy| <br/>
+---+----+ <br/>

### Filtering Data

from pyspark.sql import SparkSession <br/>
spark = SparkSession.builder.appName('Basics').getOrCreate() <br/>
df  =  spark.read.csv('/FileStore/tables/appl_stock.csv',inferSchema=True,header=True) <br/>
df.show() <br/>

+-------------------+------------------+------------------+------------------+------------------+---------+------------------+ <br/>
|               Date|              Open|              High|               Low|             Close|   Volume|         Adj Close| <br/>
+-------------------+------------------+------------------+------------------+------------------+---------+------------------+ <br/>
|2010-01-04 00:00:00|        213.429998|        214.499996|212.38000099999996|        214.009998|123432400|         27.727039| <br/>
|2010-01-05 00:00:00|        214.599998|        215.589994|        213.249994|        214.379993|150476200|27.774976000000002| <br/>
|2010-01-06 00:00:00|        214.379993|            215.23|        210.750004|        210.969995|138040000|27.333178000000004| <br/>
|2010-02-01 00:00:00|192.36999699999998|             196.0|191.29999899999999|        194.729998|187469100|         25.229131| <br/>
+-------------------+------------------+------------------+------------------+------------------+---------+------------------+ <br/>

df.filter("Close < 500").show() <br/>
This can be combined with select to select columns required for the output dataframe <br/>
df.filter("Close < 500").select(['Open','Close']).show() <br/>

This can also be achieved using: <br/>
df.filter(df['Close'] < 500).select('Volume').show() <br/>

To combine multiple condition use & for and,| for or and ~ for not <br/>
Also parantheis needs to be used to separate conditions <br/>

df.filter((df['Close'] < 200) & (df['Open'] > 200)).select('Open','Close').show() <br/>

We can use collect for collecting results as list <br/>

results = df.filter(df['Low'] == 197.16).show() <br/>

[Row(Date=datetime.datetime(2010, 1, 22, 0, 0), Open=206.78000600000001, High=207.499996, Low=197.16, Close=197.75, Volume=220441900, Adj Close=25.620401)] <br/>

We can extract the first row from list using <br/>
row = results[0] <br/>

We can convert this row data to dictionary <br/>
myDict = row.asDict() <br/>

{'Date': datetime.datetime(2010, 1, 22, 0, 0), <br/>
 'Open': 206.78000600000001, <br/>
 'High': 207.499996, <br/>
 'Low': 197.16, <br/>
 'Close': 197.75, <br/>
 'Volume': 220441900, <br/>
 'Adj Close': 25.620401} <br/>
 
 
 ### GroupBy and Aggregate
 
 df  = spark.read.csv('/FileStore/tables/sales_info.csv',inferSchema=True,header=True) <br/>
 df.show() <br/>
 +-------+-------+-----+ <br/>
|Company| Person|Sales| <br/>
+-------+-------+-----+ <br/>
|   GOOG|    Sam|200.0| <br/>
|   GOOG|Charlie|120.0| <br/>
|   GOOG|  Frank|340.0| <br/>
|   MSFT|   Tina|600.0| <br/>
|   MSFT|    Amy|124.0| <br/>
|   MSFT|Vanessa|243.0| <br/>
|     FB|   Carl|870.0| <br/>
|     FB|  Sarah|350.0| <br/>
|   APPL|   John|250.0| <br/>
|   APPL|  Linda|130.0| <br/>
|   APPL|   Mike|750.0| <br/>
|   APPL|  Chris|350.0| <br/>
+-------+-------+-----+ <br/>

We can group by company and calculate mean using below: <br/>

df.groupBy("Company").mean().show() <br/>  
 
 +-------+-----------------+ <br/>  
|Company|       avg(Sales)| <br/>  
+-------+-----------------+ <br/>  
|   APPL|            370.0| <br/>  
|   GOOG|            220.0| <br/>  
|     FB|            610.0| <br/>  
|   MSFT|322.3333333333333| <br/>  
+-------+-----------------+ <br/>  

There are many aggregate functions available with groupBy <br/>

There is also another aggregate function that we can use directly without groupBy. It takes dictionary as an argument. <br/>

df.agg({'Sales':'max'}).show() <br/>

+----------+ <br/>
|max(Sales)| <br/>
+----------+ <br/>
|     870.0| <br/>
+----------+ <br/>

### Functions
We can also import functions from Spark <br/>
These functions can be combined with df.select <br/>

from pyspark.sql.functions import countDistinct,avg,stddev <br/>
df.select(avg('Sales')).show() <br/>

+-----------------+ <br/>
|       avg(Sales)| <br/>
+-----------------+ <br/>
|360.5833333333333| <br/>
+-----------------+ <br/>

df.select(countDistinct('Sales')).show() <br/>

+---------------------+ <br/>
|count(DISTINCT Sales)| <br/>
+---------------------+ <br/>
|                   11| <br/>
+---------------------+ <br/>

You can also specify alias names for the output column: <br/>

df.select(avg('Sales').alias('Average Sales')).show() <br/>

We can also format number as shown using format_number function and choose how many digits to display after decimal point: <br/>
std_dev = df.select(stddev('Sales')) <br/>
Output will be: <br/>
+------------------+ <br/>
|stddev_samp(Sales)| <br/>
+------------------+ <br/>
|250.08742410799007| <br/>
+------------------+ <br/>
from pyspark.sql.functions import format_number <br/>
results = std_dev.select(format_number('stddev_samp(Sales)',2).alias('Std')).show() <br/>
+------+ <br/>
|   Std| <br/>
+------+ <br/>
|250.09| <br/>
+------+ <br/>


### OrderBy

We can sort dataframe by using orderBy as shown <br/>
df.orderBy("Sales") <br/>
By default it is ascending order <br/>
To order by descending: <br/>
df.orderBy(df['Sales'].desc()) <br/>

### Missing Data

We have 3 options to deal with missing data: <br/>
1. Keep the missing data as null <br/>
2. Drop the missing data points including the entire row <br/>
3. Fill missing data with some default value <br/>

df  = spark.read.csv('/FileStore/tables/ContainsNull.csv',inferSchema=True,header=True) <br/>
df.show() <br/>
+----+-----+-----+ <br/>
|  Id| Name|Sales| <br/>
+----+-----+-----+ <br/>
|emp1| John| null| <br/>
|emp2| null| null| <br/>
|emp3| null|345.0| <br/>
|emp4|Cindy|456.0| <br/>
+----+-----+-----+ <br/>


### Dropping all rows having missing data

To drop rows having missing data use <br/>
df.na.drop().show() <br/>

+----+-----+-----+ <br/>
|  Id| Name|Sales| <br/>
+----+-----+-----+ <br/>
|emp4|Cindy|456.0| <br/>
+----+-----+-----+ <br/>

### Dropping rows having missing data greater than threshold number of values

We can also specify the threshold while dropping. So if row has atleast threshold number of non null values then that row will not be dropped. <br/>
Example: <br/>
df.na.drop(thresh=2).show() <br/>

+----+-----+-----+ <br/>
|  Id| Name|Sales| <br/>
+----+-----+-----+ <br/>
|emp1| John| null| <br/>
|emp3| null|345.0| <br/>
|emp4|Cindy|456.0| <br/>
+----+-----+-----+ <br/>

### Dropping all rows having missing data using how='any'

There is another parameter how which can be passed. By default it is how='any' meaning if there is any null value in the row drop it. <br/>
df.na.drop(how='any') <br/>

### Dropping rows only if all columns missing data using how='all'

We can also change this to how='all' meaning drop the row only if all values are null or missing. <br/>
df.na.drop(how='all') <br/>

### Dropping rows having missing data only for specified columns using subset

We can also specify condition like only for certain column if the data is missing then drop the rows for other columns it does not matter. This can be done using subset: <br/>
df.na.drop(subset=['Sales']).show() <br/>

+----+-----+-----+ <br/>
|  Id| Name|Sales| <br/>
+----+-----+-----+ <br/>
|emp3| null|345.0| <br/>
|emp4|Cindy|456.0| <br/>
+----+-----+-----+ <br/>

### Filling default values for missing data without specifying subset columns

df.na.fill('No Name').show() <br/>

+----+-------+-----+ <br/>
|  Id|   Name|Sales| <br/>
+----+-------+-----+ <br/>
|emp1|   John| null| <br/>
|emp2|No Name| null| <br/>
|emp3|No Name|345.0| <br/>
|emp4|  Cindy|456.0| <br/>
+----+-------+-----+ <br/>
 
 In the above case spark applies the default value only to column types of String
 
 ### Filling default values for missing data by specifying subset columns
 
 df.na.fill('No Name',subset=['Name']).show() <br/>
 
 +----+-------+-----+ <br/>
|  Id|   Name|Sales| <br/>
+----+-------+-----+ <br/>
|emp1|   John| null| <br/>
|emp2|No Name| null| <br/>
|emp3|No Name|345.0| <br/>
|emp4|  Cindy|456.0| <br/>
+----+-------+-----+ <br/>

## Dates and Timestamps

from pyspark.sql import SparkSession <br/>
spark = SparkSession.builder.appName('dates').getOrCreate() <br/>
df  = spark.read.csv('/FileStore/tables/appl_stock.csv',inferSchema=True,header=True) <br/>
from pyspark.sql.functions import (dayofmonth,hour,dayofyear,month,year,weekofyear,format_number,date_format) <br/>

df.select(dayofmonth(df['Date'])).show() <br/>

+----------------+ <br/>
|dayofmonth(Date)| <br/>
+----------------+ <br/>
|               4| <br/>
|               5| <br/>
|               6| <br/>
|               7| <br/>
|               8| <br/>
|              11| <br/>
|              12| <br/>
|              13| <br/>
|               1| <br/>
+----------------+<br/>

Use the date related functions to extract information from timestamp type column as per the requirement.


