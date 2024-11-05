package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Task3 {

    // Mapper Class
    public static class ActiveDaysMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private Text userId = new Text();
        private IntWritable one = new IntWritable(1);
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头（假设第一行是表头）
            if (key.get() == 0) {
                return;
            }
            
            // 分割每行数据
            String[] fields = value.toString().split(",");
            
            // 提取字段
            String userIdStr = fields[0];  // 用户ID
            String directPurchaseAmt = fields[5];  // direct_purchase_amt字段
            String totalRedeemAmt = fields[8];  // total_redeem_amt字段
            
            // 检查direct_purchase_amt和total_redeem_amt是否大于0
            if (Double.parseDouble(directPurchaseAmt) > 0 || Double.parseDouble(totalRedeemAmt) > 0) {
                userId.set(userIdStr);
                context.write(userId, one);  // 如果该用户当天有活动，输出<用户ID, 1>
            }
        }
    }

    // Reducer Class
    public static class ActiveDaysReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private List<UserActiveDays> activeDaysList = new ArrayList<>();  // 用于存储用户和活跃天数
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int activeDaysCount = 0;
            // 计算该用户的活跃天数
            for (IntWritable val : values) {
                activeDaysCount += val.get();
            }
            activeDaysList.add(new UserActiveDays(key.toString(), activeDaysCount));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // 按活跃天数降序排序
            Collections.sort(activeDaysList, (a, b) -> Integer.compare(b.getActiveDays(), a.getActiveDays()));
            
            // 输出格式：<用户ID> TAB <活跃天数>
            for (UserActiveDays userActiveDays : activeDaysList) {
                context.write(new Text(userActiveDays.getUserId()), new IntWritable(userActiveDays.getActiveDays()));
            }
        }
    }

    // 辅助类，用于存储用户ID和活跃天数
    public static class UserActiveDays {
        private String userId;
        private int activeDays;

        public UserActiveDays(String userId, int activeDays) {
            this.userId = userId;
            this.activeDays = activeDays;
        }

        public String getUserId() {
            return userId;
        }

        public int getActiveDays() {
            return activeDays;
        }
    }

    public static void main(String[] args) throws Exception {
        // 配置任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Active Days Count");
        job.setJarByClass(Task3.class);

        // 设置 Mapper 和 Reducer 类
        job.setMapperClass(ActiveDaysMapper.class);
        job.setReducerClass(ActiveDaysReducer.class);

        // 设置输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 等待作业完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}