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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class Task4 {

    // Mapper Class
    public static class ActiveDaysMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Map<String, String> userConstellationMap = new HashMap<>();
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 读取用户信息文件并将星座与用户ID建立映射
            Path[] inputPaths = FileInputFormat.getInputPaths(context);
            for (Path path : inputPaths) {
                if (path.getName().contains("user_profile_table.csv")) {
                    // 读取用户信息文件，跳过表头
                    org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(context.getConfiguration());
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));  // 使用UTF-8编码
                    String line;
                    br.readLine();  // 跳过表头
                    while ((line = br.readLine()) != null) {
                        String[] fields = line.split(",");
                        String userId = fields[0].trim();
                        String constellation = fields[3].trim();
                        userConstellationMap.put(userId, constellation);  // 将用户ID和星座存入map
                    }
                    br.close();
                }
            }
        }
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 读取活跃天数文件
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                String userId = fields[0].trim();
                int activeDays = Integer.parseInt(fields[1].trim());

                // 如果用户ID在星座信息中存在
                if (userConstellationMap.containsKey(userId)) {
                    String constellation = userConstellationMap.get(userId);
                    outputKey.set(constellation);  // 设置星座名为Map输出key
                    outputValue.set(activeDays);   // 设置活跃天数为Map输出value
                    context.write(outputKey, outputValue);  // 输出<星座名, 活跃天数>
                }
            }
        }
    }

    // Reducer Class
    public static class ActiveDaysReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            
            // 计算该星座的总活跃天数和用户数量
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            // 计算平均活跃天数
            int averageActiveDays = sum / count;
            result.set(averageActiveDays);
            context.write(key, result);  // 输出格式: <星座名> TAB <平均活跃天数>
        }
    }

    public static void main(String[] args) throws Exception {
        // 配置任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Active Days by Constellation");
        job.setJarByClass(Task4.class);

        // 设置 Mapper 和 Reducer 类
        job.setMapperClass(ActiveDaysMapper.class);
        job.setReducerClass(ActiveDaysReducer.class);

        // 设置 Map 输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 Reducer 输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));  // 用户活跃文件路径
        FileInputFormat.addInputPath(job, new Path(args[1]));  // 用户信息文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[2]));  // 输出路径

        // 等待作业完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
