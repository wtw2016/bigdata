package com.wtw.hadoop.mr.dataskew.stage2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCDataSkewMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = value.toString().split("\t");
        context.write(new Text(strArr[0].toString()), new IntWritable(Integer.parseInt(strArr[1])));
    }
}
