package com.wtw.hadoop.mr.multiinputs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MultiInputsSeqMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyOut = new Text();
        IntWritable valOut = new IntWritable();
        String[] strArr = value.toString().split(" ");

        for (String str: strArr) {
            keyOut.set(str);
            valOut.set(1);
            context.write(keyOut, valOut);
        }
        context.getCounter("Mapper",UtilLog.getInfo(this, "MultiInputsSeqMapper")).increment(1);
    }
}
