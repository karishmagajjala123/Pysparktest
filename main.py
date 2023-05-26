import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.appName('Demo').getOrCreate()
df = spark.read.option("header",True).csv('C:\\Users\\GAJJALA RAJASHEKHAR\\Downloads\\test.csv')
df=df.withColumn("Gender",when(col("Gender")=="M","Male").when(col("Gender")=="F","Female").otherwise(col("Gender")))
df.show()