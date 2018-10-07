package com.wtw.hadoop.mr.multiinputs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultiInputsReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable val: values) {
            count += val.get();
        }
        context.write(key, new IntWritable(count));
        context.getCounter("Reducer",UtilLog.getInfo(this, "MultiInputsReduce")).increment(1);
    }
}
