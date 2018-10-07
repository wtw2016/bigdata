package com.wtw.hadoop.mr.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<ComboKey, NullWritable> {
    @Override
    public int getPartition(ComboKey comboKey, NullWritable nullWritable, int numPartitions) {
        return comboKey.getYear() % numPartitions;
    }
}
