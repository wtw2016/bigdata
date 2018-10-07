package com.wtw.hadoop.mr.dataskew.stage1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

public class WCDataSkewRandomPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        return new Random().nextInt(numPartitions);
    }
}
