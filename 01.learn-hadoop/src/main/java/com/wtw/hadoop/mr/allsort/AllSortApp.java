package com.wtw.hadoop.mr.allsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class AllSortApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        if (fs.exists(path)) {
            System.out.println("The existed file will be deleted!!!");
            fs.delete(path, true);
        }

        //设置job的各种属性
        job.setJobName("AllSortApp");                        //作业名称
        job.setJarByClass(AllSortApp.class);                 //搜索类
        job.setInputFormatClass(SequenceFileInputFormat.class); //设置输入格式

        //添加输入路径
        FileInputFormat.addInputPath(job,new Path("/usr/data/seq/tempture.seq"));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(args[0]));

        job.setMapperClass(MaxTempMapper.class);             //mapper类
        job.setReducerClass(MaxTempReducer.class);           //reducer类

        job.setMapOutputKeyClass(IntWritable.class);        //
        job.setMapOutputValueClass(IntWritable.class);      //

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);         //

        job.setNumReduceTasks(3);                           //reduce个数

        //设置全排序分区类
        job.setPartitionerClass(TotalOrderPartitioner.class);

        TotalOrderPartitioner.setPartitionFile(
                job.getConfiguration(),
                // conf, // error : java.lang.IllegalArgumentException: Can't read partitions file
                new Path("/usr/data/seq/par.lst"));
        /* 创建随机采样器对象
         * freq:每个key被选中的概率
         * numSapmple:抽取样本的总数
         * maxSplitSampled:最大采样切片数
        */
        // InputSampler.Sampler<IntWritable, IntWritable> sampler =
        //         new InputSampler.RandomSampler<IntWritable, IntWritable>(
        //                 0.1,
        //                 30,
        //                 3);
        InputSampler.Sampler<IntWritable, IntWritable> sampler1 =
                new InputSampler.IntervalSampler<IntWritable, IntWritable>(
                        0.5
                );
        //将sample数据写入分区文件.
        InputSampler.writePartitionFile(job, sampler1);

        job.waitForCompletion(true);
    }
}
