package com.wtw.hadoop.mr.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class MaxTempReducer extends Reducer<ComboKey, NullWritable, IntWritable, IntWritable>{
    /*
    * 1.同一个组里边的数据聚合到一起；
    * 2.一定要去重写分组对比器，就是定义如何分组，根据年份来分组，同一年份为一组
    * 3.默认情况下是根据hash来分区的，需要重写分区函数，按照年份来分区，保证同一年份进入到同一个reduce
    * */
    @Override
    protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(new IntWritable(key.getYear()), new IntWritable(key.getTemp()));
    }
}

