package com.suyao.mr.diyPartitioner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-17 18:28
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //关联jar
        job.setJarByClass(FlowBean.class);
        //关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //设置map的输出的k和v的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //设置最终输出的k和v的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:/input"));
        FileOutputFormat.setOutputPath(job, new Path("D:/output1"));
        //设置分区器
        job.setPartitionerClass(PhoneNumPartitioner.class);
        //设置ReducerTask的个数为PhoneNumPartitioner类中逻辑决定的分区和数量
        job.setNumReduceTasks(5);
        //提交Job
        job.waitForCompletion(true);
    }
}
