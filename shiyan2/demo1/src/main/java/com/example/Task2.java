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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Task2 {

    public static class AvgMapper extends Mapper<Object, Text, Text, Text> {

        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        private final Text weekdayKey = new Text();
        private final Text amtValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 拆分输入行
            String[] fields = value.toString().split("\\t");
            if (fields.length == 2) {
                String dateStr = fields[0];
                String amtStr = fields[1];

                try {
                    // 将日期转换为星期几
                    Date date = dateFormat.parse(dateStr);
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(date);

                    // 获取星期几
                    int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
                    String weekday = getWeekdayName(dayOfWeek);

                    weekdayKey.set(weekday);
                    amtValue.set(amtStr);

                    context.write(weekdayKey, amtValue);
                } catch (ParseException e) {
                    System.err.println("Error parsing date: " + e.getMessage());
                }
            }
        }

        private String getWeekdayName(int dayOfWeek) {
            switch (dayOfWeek) {
                case Calendar.SUNDAY:
                    return "Sunday";
                case Calendar.MONDAY:
                    return "Monday";
                case Calendar.TUESDAY:
                    return "Tuesday";
                case Calendar.WEDNESDAY:
                    return "Wednesday";
                case Calendar.THURSDAY:
                    return "Thursday";
                case Calendar.FRIDAY:
                    return "Friday";
                case Calendar.SATURDAY:
                    return "Saturday";
                default:
                    return "";
            }
        }
    }

    public static class AvgReducer extends Reducer<Text, Text, Text, Text> {

        private final Text result = new Text();
        private final Map<String, double[]> weekdaySums = new HashMap<>();
        private final Map<String, Integer> weekdayCounts = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalPurchase = 0;
            double totalRedeem = 0;
            int count = 0;

            for (Text val : values) {
                String[] amts = val.toString().split(",");
                totalPurchase += Double.parseDouble(amts[0]);
                totalRedeem += Double.parseDouble(amts[1]);
                count++;
            }

            // 存储结果用于排序
            weekdaySums.put(key.toString(), new double[]{totalPurchase, totalRedeem});
            weekdayCounts.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<String> weekdays = Arrays.asList("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday");
            weekdays.sort((w1, w2) -> {
                double avgPurchase1 = weekdaySums.get(w1)[0] / weekdayCounts.get(w1);
                double avgPurchase2 = weekdaySums.get(w2)[0] / weekdayCounts.get(w2);
                return Double.compare(avgPurchase2, avgPurchase1); // 降序排列
            });

            for (String weekday : weekdays) {
                double avgPurchase = weekdaySums.get(weekday)[0] / weekdayCounts.get(weekday);
                double avgRedeem = weekdaySums.get(weekday)[1] / weekdayCounts.get(weekday);
                result.set(avgPurchase + "," + avgRedeem);
                context.write(new Text(weekday), result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weekly Average Amt Statistics");

        job.setJarByClass(Task2.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
