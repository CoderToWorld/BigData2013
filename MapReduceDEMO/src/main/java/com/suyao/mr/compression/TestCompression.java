package com.suyao.mr.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.file.tfile.Compression;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.IOException;


public class TestCompression {
    /**
     * @author suyso
     * @create 2020-04-20 19:47
     */

    /**
     * 数据流的压缩：
     * 使用支持压缩的输出流将字节数据写出就实现压缩
     */
    @Test
    public void testCompression() throws IOException, ClassNotFoundException {
        //待压缩的文件
        String srcFile = "D:/input7/overwatch.mp4";
        //目标文件
        String destFile = "D:/output1/overwatch.mp4";
        //获取文件系统对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //输入流
        FSDataInputStream in = fs.open(new Path(srcFile));

        //使用的编码器
        String codecClassName = "org.apache.hadoop.io.compress.DefaultCodec";
        Class<?> codecClass = Class.forName(codecClassName);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);

        //输出流
        //codec.getDefaultExtension()  获取当前编解码器对应文件的扩展名
        FSDataOutputStream out = fs.create(new Path(destFile + codec.getDefaultExtension()));
        //包装输出流
        CompressionOutputStream codeOut = codec.createOutputStream(out);

        //流的拷贝
        IOUtils.copyBytes(in,codeOut,conf);

        //关闭
        IOUtils.closeStream(in);
        IOUtils.closeStream(codeOut);
    }
    /**
     * 数据流的解压缩：
     * 使用支持压缩的输入流将字节数据读取就实现解压缩
     */
    @Test
    public void testDeCompression() throws IOException {
        //待解压文件
        String srcFile = "d:/output1/overwatch.mp4.deflate";
        //目标文件
        String destFile = "d:/output2/overwatch.mp4";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        //获取编码解码器
        //通过输入文件的扩展名来获取编解码器
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(new Path(srcFile));

        //输入流
        FSDataInputStream in = fs.open(new Path(srcFile));
        CompressionInputStream codeIn = codec.createInputStream(in);
        //输出流
        FSDataOutputStream out = fs.create(new Path(destFile));

        //流的拷贝
        IOUtils.copyBytes(codeIn,out,conf);

        //关闭
        IOUtils.closeStream(codeIn);
        IOUtils.closeStream(out);
    }

}




