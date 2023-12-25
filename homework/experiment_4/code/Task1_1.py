from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


spark = SparkSession.builder.appName("Loan Amount Distribution").getOrCreate()


df = spark.read.csv("file:///home/lrz/financial_data/homework/experiment_4/data/application_data_clean.csv", header=True, inferSchema=True)

# 确保 AMT_CREDIT 是整数类型
df = df.withColumn("AMT_CREDIT", col("AMT_CREDIT").cast("integer"))

# 定义一个函数，用于计算每个区间
def credit_range(amount, interval=10000):
    lower_bound = (amount // interval) * interval
    upper_bound = lower_bound + interval
    return f"({lower_bound},{upper_bound})"

# 注册为 UDF
credit_range_udf = udf(credit_range, StringType())


distribution = df.withColumn("CREDIT_RANGE", credit_range_udf("AMT_CREDIT")) \
    .groupBy("CREDIT_RANGE") \
    .count() \
    .orderBy("CREDIT_RANGE")


distribution.rdd.map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile("file:///home/lrz/financial_data/homework/experiment_4/output/output1_1")


spark.stop()
