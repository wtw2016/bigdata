package com.wtw.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;

import java.util.Random;


/*
	1.SequenceFile
		Key-Value对方式。

	2.不是文本文件，是二进制文件。

	3.可切割
		因为有同步点。
		reader.sync(pos);	//定位到pos之后的第一个同步点。
		writer.sync();		//写入同步点

	4.压缩方式
		不压缩
		record压缩			//只压缩value
		块压缩				//按照多个record形成一个block.
* */

public class SeqFileTest {
    public static void main(String[] args) throws Exception {
        // save();
        // read();
        prepareData();
    }

    // 为全排序准备序列化数据
    public static void prepareData() throws Exception {
        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer writer =
                SequenceFile.createWriter(
                        fs,
                        conf,
                        new Path("/usr/data/seq/tempture.seq"),
                        IntWritable.class,
                        IntWritable.class,
                        SequenceFile.CompressionType.RECORD,
                        new GzipCodec()
                );

        for (int i = 0; i < 100; i++) {
            int year = 1970 + new Random().nextInt(100);
            int temp = -30 +  new Random().nextInt(100);
            writer.append(new IntWritable(year), new IntWritable(temp));
        }
        writer.close();
        System.out.println("prepareData finish!!!");
    }
    /*
     * 写操作
     * */
    public static void save() throws Exception {
        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer writer =
                SequenceFile.createWriter(
                        fs,
                        conf,
                        new Path("/data/2.seq"),
                        IntWritable.class,
                        Text.class,
                        SequenceFile.CompressionType.RECORD,
                        new GzipCodec()
                );

        for (int i = 0; i < 10; i++) {
            writer.append(new IntWritable(i), new Text("Tom_" + i));
            writer.sync(); // 同步点
        }
        writer.close();
        System.out.println("Save finish!!!");
    }

    /*
    * 读操作
    * 循环读出每一个键值对
    * */
    public static void read() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);

        SequenceFile.Reader reader = new SequenceFile.Reader(
                fs,
                new Path("/data/2.seq"),
                configuration
        );
        // reader.seek(130);
        // reader.sync(430); // 定位文件指针位置
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key)) {
            reader.getCurrentValue(value);
            System.out.println("(" + key.toString() + ", " + value.toString() + ")" + " pos : " + reader.getPosition());
        }
        reader.close();
    }
}
