from pyspark import SparkConf, SparkContext

# 初始化SparkContext
conf = SparkConf().setAppName("DailyFundFlow").setMaster("local[*]")
sc = SparkContext(conf=conf)

# 读取CSV文件路径
file_path = "/home/chenmiao/user_balance_table.csv"

# 加载数据
rdd = sc.textFile(file_path)

# 提取表头并过滤掉表头
header = rdd.first()
data = rdd.filter(lambda line: line != header)

# 定义目标月份
target_month = "201408"

# 解析数据并筛选出目标月份的记录
def parse_and_filter(line):
    fields = line.split(",")
    user_id = fields[0]
    report_date = fields[1]
    if report_date.startswith(target_month):
        return (user_id, report_date)
    return None

filtered_data = data.map(parse_and_filter).filter(lambda x: x is not None)

# 根据 user_id 统计每个用户的活跃天数
user_active_days = filtered_data.distinct().map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

# 筛选出活跃用户（至少 5 天）
active_users = user_active_days.filter(lambda x: x[1] >= 5)

# 统计活跃用户总数
active_user_count = active_users.count()

# 输出结果
print(f"2014年8月的活跃用户总数为：{active_user_count}")
# 停止 SparkContext
sc.stop()
