from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Loan Default Prediction").getOrCreate()


train_df = spark.read.csv("file:///home/lrz/financial_data/homework/experiment_4/data/train.csv", header=True, inferSchema=True)
test_df = spark.read.csv("file:///home/lrz/financial_data/homework/experiment_4/data/test.csv", header=True, inferSchema=True)

# 所有列除了 'SK_ID_CURR' 和 'TARGET' 都是特征
feature_cols = [c for c in train_df.columns if c not in ['SK_ID_CURR', 'TARGET']]

# 特征工程
# 对文本和离散特征进行 StringIndexer 和 OneHotEncoder 处理
indexers = [StringIndexer(inputCol=c, outputCol=c+"_indexed").fit(train_df) for c in feature_cols if train_df.schema[c].dataType == StringType()]
encoded = [OneHotEncoder(inputCol=c+"_indexed", outputCol=c+"_encoded") for c in feature_cols if train_df.schema[c].dataType == StringType()]

# 过滤掉具有大量唯一值的列
num_unique_vals = {c: train_df.select(c).distinct().count() for c in feature_cols}
filtered_feature_cols = [c for c in feature_cols if num_unique_vals[c] <= 1000]  

# 将所有处理过的特征组合成一个向量
assembler_inputs = [c+"_encoded" for c in filtered_feature_cols if train_df.schema[c].dataType == StringType()] + [c for c in filtered_feature_cols if train_df.schema[c].dataType != StringType()]
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

# 特征标准化
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)

# 模型定义
lr = LogisticRegression(featuresCol="scaledFeatures", labelCol="TARGET")

# 构建 pipeline
pipeline = Pipeline(stages=indexers + encoded + [assembler, scaler, lr])

# 模型训练
model = pipeline.fit(train_df)

# 模型评估
predictions = model.transform(test_df)

multi_evaluator = MulticlassClassificationEvaluator(labelCol="TARGET", predictionCol="prediction")
accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
f1_score = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})

print("Accuracy: ", accuracy)
print("F1 Score: ", f1_score)


spark.stop()
