package com.wtw.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyOut = new Text();
        IntWritable valOut = new IntWritable();
        String[] strArr = value.toString().split(" ");
        System.out.println("---------map----------");
        for (String str: strArr) {
            keyOut.set(str);
            valOut.set(2);
            context.write(keyOut, valOut);
        }
    }
}
