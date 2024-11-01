package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AmtStatistics {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text date = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 每行数据按TAB分割
            String[] fields = value.toString().split("\t");

            // 检查字段长度
            if (fields.length < 3) {
                return;
            }

            // 提取日期字段
            date.set(fields[0]);

            // 提取资金流入与流出量，处理缺失值
            String totalPurchaseAmt = fields[1].isEmpty() ? "0" : fields[1];
            String totalRedeemAmt = fields[2].isEmpty() ? "0" : fields[2];

            // 传递<日期, 资金流入量, 资金流出量>
            context.write(date, new Text(totalPurchaseAmt + "," + totalRedeemAmt));
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalInflow = 0;
            long totalOutflow = 0;

            // 计算每天的总流入和流出量
            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                long inflow = Long.parseLong(amounts[0]);
                long outflow = Long.parseLong(amounts[1]);
                totalInflow += inflow;
                totalOutflow += outflow;
            }

            // 输出格式：<日期> TAB <总流入量>,<总流出量>
            context.write(key, new Text(totalInflow + "," + totalOutflow));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Amt Statistics");
        job.setJarByClass(AmtStatistics.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}