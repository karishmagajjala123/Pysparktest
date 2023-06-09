
import findspark
from pyspark.sql.session import SparkSession


findspark.init()

# create a spark session
spark = SparkSession.builder.appName('SQL Practice Exercise').getOrCreate()

# create dataframes
df1 = spark.createDataFrame([
    (1, "John", "Doe", 20),
    (2, "Jane", "Doe", 19),
    (3, "Foo", "Bar", 20),
    (4, "Bar", "Foo", 19)
], ["id", "first_name", "last_name", "age"])

df2 = spark.createDataFrame([
    (1, "John", "Doe", 20),
    (2, "Jane", "Doe", 19),
    (3, "Foo", "Bar", 20),
    (4, "Bar", "Foo", 19)
], ["id", "first_name", "last_name", "age"])

# register the dataframes as temp tables
df1.registerTempTable("table1")
df2.registerTempTable("table2")

# query the data
query1 = spark.sql("SELECT * FROM table1")
query2 = spark.sql("SELECT * FROM table2")
query3 = spark.sql("SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id")
query4 = spark.sql("SELECT table1.id, table1.first_name, table2.last_name, table1.age FROM table1 INNER JOIN table2 ON table1.id = table2.id")

# show the results
query1.show()
query2.show()
query3.show()
query4.show()


###################Changed the column name###############

df1 = df1.withColumnRenamed("age", "avg_age").withColumnRenamed("id","emp_id")
df1.printSchema()

########### droped the column ##################
df2 = df2.drop("last_name") #First signature
df2.printSchema()





