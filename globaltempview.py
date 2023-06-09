
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#df.createGlobalTempView("people")


df = spark.read.csv("C:/Users/GAJJALA RAJASHEKHAR/Desktop/SQL/employees.csv")
# Displays the content of the DataFrame to stdout
#df.show()
#schema
df.printSchema()
#name of the department
df.select("_c0").show()

df.select(df['_c1'], df['_c0'] + 1).show()


################# GlobalTempView ############################

df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()


spark.newSession().sql("SELECT _c0, _c1 FROM global_temp.people").show()