from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, dayofmonth, dayofweek, month
from pyspark.sql.types import IntegerType, StructType, StructField, LongType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
import datetime

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ImprovedPrediction") \
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

# 添加日期特征
@udf(IntegerType())
def extract_day_of_year(date):
    date_str = str(date)
    dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    return dt.timetuple().tm_yday

@udf(IntegerType())
def extract_month(date):
    date_str = str(date)
    dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    return dt.month

@udf(IntegerType())
def extract_day_of_week(date):
    date_str = str(date)
    dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    return dt.weekday() + 1  # Monday=1, Sunday=7

data = data.withColumn("day_of_year", extract_day_of_year(col("report_date"))) \
           .withColumn("month", extract_month(col("report_date"))) \
           .withColumn("day_of_week", extract_day_of_week(col("report_date")))

# 准备特征和标签
assembler = VectorAssembler(
    inputCols=["day_of_year", "month", "day_of_week"],
    outputCol="features"
)
data = assembler.transform(data)

# 分别为申购和赎回创建训练数据
purchase_data = data.select(col("features"), col("total_purchase_amt").alias("label"))
redeem_data = data.select(col("features"), col("total_redeem_amt").alias("label"))

# 创建并训练随机森林模型
purchase_rf = RandomForestRegressor(numTrees=100, maxDepth=10, seed=42)
purchase_model = purchase_rf.fit(purchase_data)

redeem_rf = RandomForestRegressor(numTrees=100, maxDepth=10, seed=42)
redeem_model = redeem_rf.fit(redeem_data)

# 生成2014年9月的预测日期
start_date = datetime.datetime.strptime("20140901", "%Y%m%d")
dates = [(start_date + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(30)]
dates_df = spark.createDataFrame([(int(date),) for date in dates], ["report_date"])

dates_df = dates_df.withColumn("day_of_year", extract_day_of_year(col("report_date"))) \
                   .withColumn("month", extract_month(col("report_date"))) \
                   .withColumn("day_of_week", extract_day_of_week(col("report_date")))

dates_df = assembler.transform(dates_df)

# 预测申购总额
purchase_predictions = purchase_model.transform(dates_df).select(
    col("report_date"), col("prediction").cast(LongType()).alias("purchase")
)

# 预测赎回总额
redeem_predictions = redeem_model.transform(dates_df).select(
    col("report_date"), col("prediction").cast(LongType()).alias("redeem")
)

# 合并结果
results = purchase_predictions.join(redeem_predictions, ["report_date"])

# 保存为CSV文件
results_list = results.collect()
with open(output_path, 'w') as f:
    f.write("report_date,purchase,redeem\n")
    for row in results_list:
        f.write(f"{int(row['report_date'])},{int(row['purchase'])},{int(row['redeem'])}\n")

print(f"Prediction results saved to {output_path}")

# 停止SparkSession
spark.stop()
