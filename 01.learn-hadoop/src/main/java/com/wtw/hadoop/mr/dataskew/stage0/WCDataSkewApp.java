package com.wtw.hadoop.mr.dataskew.stage0;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
* 本示例展示的是数据倾斜问题
*    1.其中某一个reduce中，数据大量聚集
* */

public class WCDataSkewApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage : <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[1]);
        if (fs.exists(path)) {
            System.out.println("The existed file will be deleted!!!");
            fs.delete(path, true);
        }

        Job job = Job.getInstance(conf);
        // 设置job的各种属性
        job.setJobName("WCDataSkewApp");
        job.setJarByClass(WCDataSkewApp.class);
        job.setInputFormatClass(TextInputFormat.class);

        // 添加需要处理文件的路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置mapper/Reducer类
        job.setMapperClass(WCDataSkewMapper.class);
        job.setReducerClass(WCDataSkewReduce.class);

        // 设置reduce的个数
        job.setNumReduceTasks(3);

        // 设置map输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置输出结果的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
