package com.wtw.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestSeqFile {
    /*
    * 写操作
    * */
    @Test
    public void save() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer writer =
        SequenceFile.createWriter(
            fs,
            conf,
            new Path("d:/data/1.seq"),
            IntWritable.class,
            Text.class
        );

        for (int i = 0; i < 100; i++) {
            writer.append(new IntWritable(i), "Tom_" + i);
        }
        writer.close();
    }
}
