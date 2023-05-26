import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.appName('Demo').getOrCreate()
df = spark.read.option("header",True).csv('C:\\Users\\GAJJALA RAJASHEKHAR\\Downloads\\employees.csv')

#show the complete table
#df.show()

# Aggregate of the sum of the salary
#df.agg(sum("SALARY").alias("SUM_SALARY")).show()

#Group by department id and sum of the salary
#df.groupBy("DEPARTMENT_ID").agg(sum("SALARY").alias("SUM_SALARY")).filter(col("SUM_SALARY")>10000).sort(desc("SUM_SALARY")).show()

#Using Windows found the Average,Sum,Min and Max

#windowSpec  = Window.partitionBy("DEPARTMENT_ID").orderBy("SALARY")
#windowSpecAgg  = Window.partitionBy("DEPARTMENT_ID")
#df.withColumn("row",row_number().over(windowSpec)) \
#  .withColumn("avg", avg(col("SALARY")).over(windowSpecAgg)) \
#  .withColumn("sum", sum(col("SALARY")).over(windowSpecAgg)) \
# .withColumn("min", min(col("SALARY")).over(windowSpecAgg)) \
# .withColumn("max", max(col("SALARY")).over(windowSpecAgg)) \
#  .where(col("row")==1).select("DEPARTMENT_ID","avg","sum","min","max") \
#  .show()

# Using Inner Joint operation
df1 = spark.read.option("header", True).csv('C:\\Users\\GAJJALA RAJASHEKHAR\\Downloads\\department.csv')

#df.join(df1,df.DEPARTMENT_ID == df1.DEPARTMENT_ID,"inner").show(truncate=False)

#df.join(df1,df.DEPARTMENT_ID == df1.DEPARTMENT_ID,"leftsemi").show(truncate=False)
#df.join(df1,df.DEPARTMENT_ID == df1.DEPARTMENT_ID,"leftanti").show(truncate=False)

#df.join(df1,df.DEPARTMENT_ID == df1.DEPARTMENT_ID,"right").show(truncate=False)
#df.join(df1,df.DEPARTMENT_ID == df1.DEPARTMENT_ID,"left").show(truncate=False)
#df.join(df1,df.DEPARTMENT_ID == df1.DEPARTMENT_ID,"outer").show(truncate=False)

#########using SQL Query#####################
#df.createOrReplaceTempView("EMP")
#df1.createOrReplaceTempView("DEPT")
#joinDF = spark.sql("select e.EMPLOYEE_ID,e.FIRST_NAME,e.JOB_ID,e.DEPARTMENT_ID,d.DEPARTMENT_NAME  from EMP e, DEPT d where e.DEPARTMENT_ID == d.DEPARTMENT_ID") \
#  .show(truncate=False)
#joinDF2 = spark.sql("select e.EMPLOYEE_ID,e.FIRST_NAME,e.JOB_ID,e.DEPARTMENT_ID,d.DEPARTMENT_NAME from EMP e INNER JOIN DEPT d ON e.DEPARTMENT_ID == d.DEPARTMENT_ID") \
#  .show(truncate=False)
