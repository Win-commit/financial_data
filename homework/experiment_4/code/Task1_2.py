from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Loan Analysis").getOrCreate()


df = spark.read.csv("file:///home/lrz/financial_data/homework/experiment_4/data/application_data_clean.csv", header=True, inferSchema=True)

# 计算差值
df = df.withColumn("DIFFERENCE", col("AMT_CREDIT") - col("AMT_INCOME_TOTAL"))

# 获取差值最高的10条记录
highest_diff = df.orderBy(col("DIFFERENCE").desc()).limit(10)

# 获取差值最低的10条记录
lowest_diff = df.orderBy(col("DIFFERENCE").asc()).limit(10)


result = highest_diff.union(lowest_diff)


result = result.select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "DIFFERENCE")



result.coalesce(1).write.format("csv").option("header", "true").save("file:///home/lrz/financial_data/homework/experiment_4/output/output1_2")
