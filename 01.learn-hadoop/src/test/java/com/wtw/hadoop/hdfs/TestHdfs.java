package com.wtw.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestHdfs {
    @Test
    public void test1() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        // Path path = new Path("hdfs://192.168.1.110:8020/");
        FSDataOutputStream fout = fs.create(new Path("/usr/data/txt.txt"));
        fout.write("How are you?\n".getBytes());
        fout.close();
    }

    @Test
    public void test2() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        FSDataOutputStream fout = null;
        fout = fs.create(new Path("/usr/data/txt1.txt"),
                true,
                1024,
                (short) 2,
                1024*1024);
        fout.write("How are you?\n".getBytes());
        fout.close();
    }

}
