package com.wtw.hadoop.mr.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySortApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage : <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[1]);
        if (fs.exists(path)) {
            System.out.println("The existed file will be deleted!!!");
            fs.delete(path, true);
        }

        //设置job的各种属性
        job.setJobName("SecondarySortApp");                        //作业名称
        job.setJarByClass(SecondarySortApp.class);                 //搜索类
        // job.setInputFormatClass(SequenceFileInputFormat.class); //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(MaxTempMapper.class);             //mapper类
        job.setReducerClass(MaxTempReducer.class);           //reducer类

        job.setMapOutputKeyClass(ComboKey.class);        //
        job.setMapOutputValueClass(NullWritable.class);      //

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);         //

        // reduce的个数决定了map的分区数目
        job.setNumReduceTasks(3);                           //reduce个数
        //分区函数类，分组比较器，排序比较器
        job.setPartitionerClass(YearPartitioner.class);
        job.setGroupingComparatorClass(YearGroupComparator.class);
        job.setSortComparatorClass(ComboKeySortComparator.class);

        System.out.println(new ComboKey());

        job.waitForCompletion(true);
    }
}
