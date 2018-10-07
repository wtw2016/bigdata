package com.wtw.hadoop.mr.multiinputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiInputsApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            System.out.println("Usage: <output path>");
            return;
        }
        // delete output files
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        if (fs.exists(path)) {
            System.out.println("The existed file will be deleted!!!");
            fs.delete(path, true);
        }

        Job job = Job.getInstance(conf);
        // 设置job的各种属性
        job.setJobName("MultiInputsApp");
        job.setJarByClass(MultiInputsApp.class);

        // 设置多个输入
        MultipleInputs.addInputPath(job,
                                    new Path("/usr/data/text"),
                                    TextInputFormat.class,
                                    MultiInputsTextMapper.class
                                );
        MultipleInputs.addInputPath(job,
                                    new Path("/usr/data/seq"),
                                    SequenceFileInputFormat.class,
                                    MultiInputsSeqMapper.class
                                );
        // 设置输出格式
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setReducerClass(MultiInputsReduce.class); //reducer类
        job.setNumReduceTasks(3); //reduce个数

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);     //

        job.waitForCompletion(true);
    }
}
