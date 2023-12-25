from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化 Spark Session
spark = SparkSession.builder.appName("Average Income Calculation").getOrCreate()

# 加载数据
df = spark.read.csv("file:///home/lrz/financial_data/homework/experiment_4/data/application_data_clean.csv", header=True, inferSchema=True)

# 计算每个客户自出生以来每天的平均收入
df = df.withColumn("avg_income", col("AMT_INCOME_TOTAL") / -col("DAYS_BIRTH"))

# 筛选日收入大于 1 的客户，并按日收入从大到小排序
filtered_df = df.filter(df['avg_income'] > 1).orderBy(col("avg_income").desc())

# 选择需要的列
result_df = filtered_df.select("SK_ID_CURR", "avg_income")

# 将结果保存到带有标题的 .txt 文件
result_df.coalesce(1).write.format("csv").option("header", "true").save("file:///home/lrz/financial_data/homework/experiment_4/output/output2_2")

# 停止 Spark Session
spark.stop()
