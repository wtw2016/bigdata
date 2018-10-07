package com.wtw.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/*
* 	1.Key-value
	2.key按升序写入(可重复)。
	3.mapFile对应一个目录，目录下有index和data文件,都是序列文件。
	4.index文件划分key区间,用于快速定位。
* */

public class MapFileTest {
    public static void main(String[] args) throws Exception {
        write();
        read();
    }

    public static void write() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        MapFile.Writer writer = new MapFile.Writer(
                conf,
                fs,
                "/data/map",
                IntWritable.class,
                Text.class,
                SequenceFile.CompressionType.BLOCK
                // new GzipCodec()
        );
        for (int i = 0; i < 100; i++){
            writer.append(new IntWritable(i), new Text("jom_"+i));
        }
        writer.close();
    }

    public static void read() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        MapFile.Reader reader = new MapFile.Reader(
                fs,
                "/data/map",
                conf
        );
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key, value)) {
            System.out.println("(" + key.toString() + ", " + value.toString() + ")");
        }
        reader.close();
    }
}
