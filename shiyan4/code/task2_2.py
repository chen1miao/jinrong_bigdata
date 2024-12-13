from pyspark.sql import SparkSession

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("TopUsersByFlow") \
    .master("local[*]") \
    .getOrCreate()

# 注册 DataFrame 为临时视图
user_profile_df = spark.read.csv("/home/chenmiao/user_profile_table.csv", header=True, inferSchema=True)
user_balance_df = spark.read.csv("/home/chenmiao/user_balance_table.csv", header=True, inferSchema=True)

user_profile_df.createOrReplaceTempView("user_profile_table")
user_balance_df.createOrReplaceTempView("user_balance_table")

# 使用Spark SQL执行查询
query = """
WITH user_city_activity AS (
  SELECT up.user_id, up.city, 
         (SUM(ub.total_purchase_amt) + SUM(ub.total_redeem_amt)) AS total_traffic
  FROM user_balance_table ub
  JOIN user_profile_table up ON ub.user_id = up.user_id
  WHERE ub.report_date >= '20140801' AND ub.report_date <= '20140831'
  GROUP BY up.user_id, up.city
),

ranked_users AS (
  SELECT user_id, city, total_traffic,
         ROW_NUMBER() OVER (PARTITION BY city ORDER BY total_traffic DESC) AS rank
  FROM user_city_activity
)

SELECT CAST(user_id AS BIGINT) AS user_id, 
       city, 
       CAST(total_traffic AS BIGINT) AS total_traffic
FROM ranked_users
WHERE rank <= 3
ORDER BY city, rank;
"""

# 执行查询并获取结果
top_users_df = spark.sql(query)

# 收集结果到本地
results = top_users_df.collect()

# 输出结果到终端
for row in results:
    print(f"{row['city']}\t{row['user_id']}\t{row['total_traffic']}")

# 保存结果为CSV文件
output_path = "/home/chenmiao/jinrong_bigdata/shiyan4/output/task2_2.csv"
with open(output_path, 'w') as f:
    f.write("City\tuser_id\ttotal_flow\n")
    for row in results:
        f.write(f"{row['city']}\t{row['user_id']}\t{row['total_traffic']}\n")

# 停止SparkSession
spark.stop()
