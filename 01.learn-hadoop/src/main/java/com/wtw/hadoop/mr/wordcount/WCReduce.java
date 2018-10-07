package com.wtw.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable val: values) {
            count += val.get() + 1;
        }
        String tno = Thread.currentThread().getName();
        System.out.println(tno + " : WCReducer :" + key.toString() + "=" + count);
        context.write(key, new IntWritable(count));
    }
}
