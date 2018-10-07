package com.wtw.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCApp {
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

        // conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        // 设置job的各种属性
        job.setJobName("WCApp");
        job.setJarByClass(WCApp.class);
        job.setInputFormatClass(TextInputFormat.class);
        // 设置reduce输出格式
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 添加需要处理文件的路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置mapper/Reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReduce.class);

        //
        // MultipleInputs.addInputPath(
        //         job,
        //
        // );

        // 设置分区函数和combiner函数
        System.out.println("++++k++++++++++++");
        job.setPartitionerClass(MyPartitioner.class);
        job.setCombinerClass(WCReduce.class);

        // 设置最大最小切片
        // FileInputFormat.setMinInputSplitSize(job, 1);
        // FileInputFormat.setMaxInputSplitSize(job, 13);

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
