import pandas as pd
from sklearn.model_selection import train_test_split

# 读取 CSV 文件
df = pd.read_csv('/home/lrz/financial_data/homework/experiment_4/data/application_data_clean.csv')

# 分割数据集为训练集和测试集
train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
train_df.to_csv("/home/lrz/financial_data/homework/experiment_4/data/train.csv")
test_df.to_csv("/home/lrz/financial_data/homework/experiment_4/data/test.csv")