package com.wtw.hadoop.mr.secondarysort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WCTextMapper
 */
public class MaxTempMapper extends Mapper<LongWritable, Text, ComboKey, NullWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arrs = line.split(" ");

        ComboKey keyOut = new ComboKey();
        keyOut.setYear(Integer.parseInt(arrs[0]));
        keyOut.setTemp(Integer.parseInt(arrs[1]));
        context.write(keyOut, NullWritable.get());

        // for seq file
        /*ComboKey keyOut = new ComboKey();
        keyOut.setYear(key.get());
        keyOut.setTemp(value.get());
        context.write(keyOut, NullWritable.get());
        */
    }
}
