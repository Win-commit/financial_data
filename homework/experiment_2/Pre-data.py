import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

drop_columns=["FLAG_EMP_PHONE","FLAG_WORK_PHONE","FLAG_PHONE","REG_REGION_NOT_LIVE_REGION","REG_REGION_NOT_WORK_REGION","LIVE_REGION_NOT_WORK_REGION","OBS_30_CNT_SOCIAL_CIRCLE","OBS_60_CNT_SOCIAL_CIRCLE","DEF_60_CNT_SOCIAL_CIRCLE",  "DAYS_BIRTH","NAME_INCOME_TYPE"]
df = pd.read_csv("fe-course-data/application_data_clean.csv")
df=df.drop(drop_columns,axis=1)

numeric_columns = ["AMT_CREDIT","AMT_INCOME_TOTAL","REGION_POPULATION_RELATIVE"]

for columnName in numeric_columns:
    # 计算四分位数
    Q1 = df[columnName].quantile(0.25)
    Q2 = df[columnName].quantile(0.50) 
    Q3 = df[columnName].quantile(0.75)

    # 划分为四个等级
    bins = [df[columnName].min() - 1, Q1, Q2, Q3, df[columnName].max()]
    labels = [1, 2, 3, 4]
    df[columnName] = pd.cut(df[columnName], bins=bins, labels=labels, include_lowest=True)

train_size = int(0.8 * len(df))
train = df[:train_size]
test = df[train_size:]



train.to_csv("fe-course-data/application_data_train1.csv", index=False)
test.to_csv("fe-course-data/application_data_test1.csv", index=False)