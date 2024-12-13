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

# 解析数据，将缺失值处理为零并提取需要的字段（report_date, total_purchase_amt, total_redeem_amt）
def parse_line(line):
    fields = line.split(",")
    report_date = fields[1]  # report_date是第二个字段
    try:
        total_purchase_amt = int(fields[4]) if fields[4] else 0  # total_purchase_amt是第五个字段
    except ValueError:
        total_purchase_amt = 0
    try:
        total_redeem_amt = int(fields[8]) if fields[8] else 0  # total_redeem_amt是第九个字段
    except ValueError:
        total_redeem_amt = 0
    return (report_date, (total_purchase_amt, total_redeem_amt))

# 转换数据格式
data_parsed = data.map(parse_line)

# 按日期聚合，计算总资金流入和流出
data_aggregated = data_parsed.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)
results = data_aggregated.sortByKey().collect() # 排序

# 保存为CSV文件
output_path = "/home/chenmiao/jinrong_bigdata/shiyan4/output/task1_1.csv"
with open(output_path, 'w') as f:
    f.write("report_date,total_purchase_amt,total_redeem_amt\n")
    for report_date, (total_purchase_amt, total_redeem_amt) in results:
        f.write(f"{report_date},{int(total_purchase_amt)},{int(total_redeem_amt)}\n")

# 格式化输出结果
result_terminal = [f"{x[0]}\t{x[1][0]}\t{x[1][1]}" for x in results]

# 输出结果到终端
for line in result_terminal:
    print(line)

# 停止SparkContext
sc.stop()

print(f"每日资金流入与流出统计已完成，结果保存到：{output_path}")
