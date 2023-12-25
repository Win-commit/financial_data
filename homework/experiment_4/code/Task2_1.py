from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round

# 初始化 Spark Session
spark = SparkSession.builder.appName("Gender Children Statistics").getOrCreate()

# 加载数据
df = spark.read.csv("file:///home/lrz/financial_data/homework/experiment_4/data/application_data_clean.csv", header=True, inferSchema=True)

# 筛选男性客户并计算孩子数量的分布
gender_filtered = df.filter(df['CODE_GENDER'] == 'M')
children_count = gender_filtered.groupBy("CNT_CHILDREN").count()

# 计算总的男性客户数
total_males = gender_filtered.count()

# 计算每种孩子数量的占比
children_ratio = children_count.withColumn("ratio", round((col("count") / total_males), 4))
children_ratio = children_ratio.select("CNT_CHILDREN", "ratio").orderBy("CNT_CHILDREN")


rdd = children_ratio.rdd.map(lambda x: ','.join(str(i) for i in x))

# 保存RDD为文本文件
rdd.saveAsTextFile("file:///home/lrz/financial_data/homework/experiment_4/output/output2_1")

# 停止 Spark Session
spark.stop()
