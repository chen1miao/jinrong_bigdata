package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AmtStatistics {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text dateKey = new Text();
        private Text amtValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 拆分行
            String[] fields = value.toString().split(",");

            // 检查是否为标题行以及字段数目是否满足条件，确保不越界访问
            if (fields.length >= 9 && !fields[4].equals("total_purchase_amt")) {
                String date = fields[1]; // 第二列为report_date
                String purchaseAmt = fields[4]; // 第五列为total_purchase_amt
                String redeemAmt = fields[8]; // 第九列为total_redeem_amt

                // 处理缺失值，将其视为零交易
                if (purchaseAmt.isEmpty()) {
                    purchaseAmt = "0";
                }
                if (redeemAmt.isEmpty()) {
                    redeemAmt = "0";
                }

                // 尝试将字符串转换为数字，确保没有无效数据
                try {
                    Double.parseDouble(purchaseAmt);
                    Double.parseDouble(redeemAmt);
                } catch (NumberFormatException e) {
                    return; // 忽略含有无效数字的行
                }

                dateKey.set(date);
                amtValue.set(purchaseAmt + "," + redeemAmt);

                context.write(dateKey, amtValue);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalPurchase = 0;
            double totalRedeem = 0;

            for (Text val : values) {
                String[] amts = val.toString().split(",");
                totalPurchase += Double.parseDouble(amts[0]);
                totalRedeem += Double.parseDouble(amts[1]);
            }

            // 输出格式: <日期> TAB <资金流入量>,<资金流出量>
            result.set(totalPurchase + "," + totalRedeem);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Amt Statistics");

        job.setJarByClass(AmtStatistics.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumReducer.class); // 可选，用于本地先行汇总
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}