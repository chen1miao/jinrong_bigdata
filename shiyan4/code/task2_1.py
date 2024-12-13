from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc
import csv

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("CityAverageBalance") \
    .master("local[*]") \
    .getOrCreate()

# 文件路径
profile_table_path = "/home/chenmiao/user_profile_table.csv"
balance_table_path = "/home/chenmiao/user_balance_table.csv"
output_path = "/home/chenmiao/jinrong_bigdata/shiyan4/output/task2_1.csv"

# 目标日期
target_date = "20140301"

# 读取 user_profile_table
user_profile_df = spark.read.csv(profile_table_path, header=True, inferSchema=True)

# 读取 user_balance_table
user_balance_df = spark.read.csv(balance_table_path, header=True, inferSchema=True)

# 筛选目标日期的数据
filtered_balance_df = user_balance_df.filter(col("report_date") == target_date)

# 将两个表按 user_id 进行关联
joined_df = user_profile_df.join(filtered_balance_df, on="user_id", how="inner")

# 按城市计算平均余额
city_avg_balance_df = joined_df.groupBy("City") \
    .agg(avg(col("tBalance")).alias("average_balance")) \
    .orderBy(desc("average_balance"))

# 收集结果到本地
results = city_avg_balance_df.collect()

# 保存结果为CSV文件
with open(output_path, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # 写入表头
    writer.writerow(["City", "average_balance"])
    # 写入数据
    for row in results:
        writer.writerow([row["City"], row["average_balance"]])

# 输出结果到终端
for row in results:
    print(f"{row['City']}\t{row['average_balance']}")

# 停止SparkSession
spark.stop()

print(f"每个城市在2014年3月1日的平均余额计算已完成，结果保存到：{output_path}")