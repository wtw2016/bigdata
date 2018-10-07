package com.wtw.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

    public MyPartitioner(){
        System.out.println("new MyPartitioner");
    }

    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        System.out.println("MyPartitioner...");
        return 0;
    }
}
