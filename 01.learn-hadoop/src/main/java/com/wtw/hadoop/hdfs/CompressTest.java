package com.wtw.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class CompressTest {
    public static void main(String[] args) throws Exception {
        Class[] cls = {
                // DefaultCodec.class,
                // GzipCodec.class,
                // BZip2Codec.class,
                // Lz4Codec.class,
                SnappyCodec.class,
                LzopCodec.class
        };
        for (Class c : cls) {
            Compress(c);
        }
    }

    /*
     * 压缩
     * */
    public static void Compress(Class classCodec) throws Exception{
        long start = System.currentTimeMillis();
        // 实例化压缩编解码实例
        CompressionCodec codec = (CompressionCodec)
                ReflectionUtils.newInstance(classCodec, new Configuration());
        // 得到文件扩展名，创建文件输出流
        FileOutputStream fos = new FileOutputStream("/home/book/data/2" + codec.getDefaultExtension());

        // 得到压缩流
        CompressionOutputStream cos = codec.createOutputStream(fos);
        IOUtils.copyBytes(new FileInputStream("/home/book/data/1.pdf"), cos, 1024);
        fos.close();
        System.out.println(classCodec.getSimpleName() + " : " + (System.currentTimeMillis() - start));
    }
}

