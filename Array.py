from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType,StructType,StructField
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import array

spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()


arrayCol = ArrayType(StringType(),False)

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

schema = StructType([
    StructField("name",StringType(),True),
    StructField("languagesAtSchool",ArrayType(StringType()),True),
    StructField("languagesAtWork",ArrayType(StringType()),True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()


df.select(df.name,explode(df.languagesAtSchool)).show()


df.select(split(df.name,",").alias("nameAsArray")).show()


df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()


df.select(df.name,df.languagesAtSchool,array_contains(df.languagesAtSchool,"Java")
    .alias("array_contains")).show()