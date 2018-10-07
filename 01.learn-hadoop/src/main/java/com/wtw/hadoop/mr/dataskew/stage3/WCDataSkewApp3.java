package com.wtw.hadoop.mr.dataskew.stage3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
* 本示例是在上一个示例stage2的基础之上使用key-value来作为输入
*    1.在stage1的基础上解决统计结果不准确的问题
*    2.将part-r-xxxxx文件逐个处理；
*    3.不使用自定义的分区，而是采用默认的hash计算来分区
* */
public class WCDataSkewApp3 {
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
        job.setJobName("WCDataSkewApp3");
        job.setJarByClass(WCDataSkewApp3.class);
        /*
        * 使用key-value输入类型
        * */
        // job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 添加需要处理文件的路径
        FileInputFormat.addInputPath(job, new Path("/usr/data/output/part-r-00000"));
        FileInputFormat.addInputPath(job, new Path("/usr/data/output/part-r-00001"));
        FileInputFormat.addInputPath(job, new Path("/usr/data/output/part-r-00002"));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        // 设置mapper/Reducer类
        job.setMapperClass(WCDataSkewMapper3.class);
        job.setReducerClass(WCDataSkewReduce3.class);

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
        // 提交job到集群
        job.waitForCompletion(true);
    }
}
