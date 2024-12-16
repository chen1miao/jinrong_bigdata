from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofyear, month, dayofmonth, weekofyear, to_date, unix_timestamp
from pyspark.sql.types import StructType, StructField, LongType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import datetime

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("FinancePrediction") \
    .master("local[*]") \
    .getOrCreate()

# 定义数据路径和输出路径
input_path = "/home/chenmiao/jinrong_bigdata/shiyan4/output/data_for_task3.csv"
output_path = "/home/chenmiao/jinrong_bigdata/shiyan4/output/tc_comp_predict_table.csv"

# 定义数据结构
schema = StructType([
    StructField("report_date", LongType(), True),  
    StructField("total_purchase_amt", LongType(), True),
    StructField("total_redeem_amt", LongType(), True)
])

# 读取数据
data = spark.read.csv(input_path, schema=schema, header=True)

# 将report_date从BIGINT转换为DATE类型
data = data.withColumn("report_date", to_date(col("report_date"), "yyyyMMdd"))

# 添加日期特征
data = data.withColumn("day_of_year", dayofyear(col("report_date")))
data = data.withColumn("month", month(col("report_date")))
data = data.withColumn("day_of_month", dayofmonth(col("report_date")))
data = data.withColumn("week_of_year", weekofyear(col("report_date")))

# 准备特征和标签
assembler = VectorAssembler(inputCols=["day_of_year", "month", "day_of_month", "week_of_year"], outputCol="features")
data = assembler.transform(data)

# 分别为申购和赎回创建训练数据
purchase_data = data.select(col("features"), col("total_purchase_amt").alias("label"))
redeem_data = data.select(col("features"), col("total_redeem_amt").alias("label"))

# 创建随机森林模型
rf = RandomForestRegressor(featuresCol="features", labelCol="label")

# 参数调优
paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [10, 20, 50]) \
    .addGrid(rf.maxDepth, [4, 8, 12]) \
    .build()

evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")

# 交叉验证
cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)
cv_model_purchase = cv.fit(purchase_data)
cv_model_redeem = cv.fit(redeem_data)

# 生成2014年9月的预测日期
start_date = datetime.datetime.strptime("20140901", "%Y%m%d")
dates = [(start_date + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(30)]
dates_df = spark.createDataFrame([(int(date),) for date in dates], ["report_date"])

# 将report_date从BIGINT转换为DATE类型
dates_df = dates_df.withColumn("report_date", to_date(col("report_date"), "yyyyMMdd"))

# 添加日期特征
dates_df = dates_df.withColumn("day_of_year", dayofyear(col("report_date")))
dates_df = dates_df.withColumn("month", month(col("report_date")))
dates_df = dates_df.withColumn("day_of_month", dayofmonth(col("report_date")))
dates_df = dates_df.withColumn("week_of_year", weekofyear(col("report_date")))

# 特征向量组装
dates_df = assembler.transform(dates_df)

# 预测申购总额
purchase_predictions = cv_model_purchase.transform(dates_df).select(
    col("report_date"), col("prediction").cast(LongType()).alias("purchase")
)

# 预测赎回总额
redeem_predictions = cv_model_redeem.transform(dates_df).select(
    col("report_date"), col("prediction").cast(LongType()).alias("redeem")
)

# 合并结果
results = purchase_predictions.join(redeem_predictions, ["report_date"])

# 将DATE类型的report_date转换回BIGINT类型
results = results.withColumn("report_date", unix_timestamp(col("report_date"), "yyyyMMdd").cast("long"))

# 保存为CSV文件
results_list = results.collect()
with open(output_path, 'w') as f:
    f.write("report_date,purchase,redeem\n")
    for row in results_list:
        f.write(f"{row['report_date']},{row['purchase']},{row['redeem']}\n")

print(f"Prediction results saved to {output_path}")

# 停止SparkSession
spark.stop()