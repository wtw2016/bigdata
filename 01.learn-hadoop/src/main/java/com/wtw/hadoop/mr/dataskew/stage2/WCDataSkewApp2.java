package com.wtw.hadoop.mr.dataskew.stage2;

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
* 本示例是在上一个示例stage1的基础之上在做一次job任务
*    1.在stage1的基础上解决统计结果不准确的问题
*    2.将part-r-xxxxx文件逐个处理；
*    3.不使用自定义的分区，而是采用默认的hash计算来分区
* */
public class WCDataSkewApp2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        if (fs.exists(path)) {
            System.out.println("The existed file will be deleted!!!");
            fs.delete(path, true);
        }

        Job job = Job.getInstance(conf);
        // 设置job的各种属性
        job.setJobName("WCDataSkewApp2");
        job.setJarByClass(WCDataSkewApp2.class);
        job.setInputFormatClass(TextInputFormat.class);

        // 添加需要处理文件的路径
        FileInputFormat.addInputPath(job, new Path("/usr/data/output/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("/usr/data/output/part-r-00001"));
        FileInputFormat.addInputPath(job, new Path("/usr/data/output/part-r-00002"));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        // 设置mapper/Reducer类
        job.setMapperClass(WCDataSkewMapper2.class);
        job.setReducerClass(WCDataSkewReduce2.class);

        /*
        * 设置分区函数
        * */
        // job.setPartitionerClass(WCDataSkewRandomPartitioner.class);

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
