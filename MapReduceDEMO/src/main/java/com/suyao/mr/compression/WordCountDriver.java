package com.suyao.mr.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-14 18:40
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 创建一个Job对象
        Configuration conf = new Configuration();

        //开启mapper输出数据压缩
        conf.set("mapreduce.map.output.compress","true");
        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");

        //开启reduce输出数据的压缩
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");
        Job job = Job.getInstance(conf);
        //2. 关联jar
        //job.setJarByClass(WordCountDriver.class);
        job.setJar("D:\\woekspace_idea\\BigData2013\\MapReduceDEMO\\target\\MapReduceDEMO-1.0-SNAPSHOT.jar");

        //3. 关联Mapper 和 Reducer 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4. 设置Mapper的输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5. 设置最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6. 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:/input7"));
        FileOutputFormat.setOutputPath(job,new Path("D:/output2")); // 输出路径不能存在，如果已经存在就报异常.


        //7. 提交job
        job.waitForCompletion(true);
    }
}
