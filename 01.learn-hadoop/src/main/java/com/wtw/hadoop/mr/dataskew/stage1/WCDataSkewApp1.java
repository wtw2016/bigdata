package com.wtw.hadoop.mr.dataskew.stage1;

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
* 本示例是在上一个示例的基础之上采用自定义分区的函数来实现数据均衡
*    1.使用随机值来确定分区；
*    2.但是带来了单词统计的结果不准确
* */
public class WCDataSkewApp1 {
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
        job.setJobName("WCDataSkewApp1");
        job.setJarByClass(WCDataSkewApp1.class);
        job.setInputFormatClass(TextInputFormat.class);

        // 添加需要处理文件的路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置mapper/Reducer类
        job.setMapperClass(WCDataSkewMapper1.class);
        job.setReducerClass(WCDataSkewReduce1.class);

        /*
        * 设置分区函数
        * */
        job.setPartitionerClass(WCDataSkewRandomPartitioner.class);

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
