
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 
# Create RDD from parallelize    
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd=spark.sparkContext.parallelize(dataList)
print(rdd)

##OutPut
#ParallelCollectionRDD[37] at readRDDFromFile at PythonRDD.scala:287

data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd1=spark.sparkContext.parallelize(data)
print(rdd1)

#ParallelCollectionRDD[39] at readRDDFromFile at PythonRDD.scala:287