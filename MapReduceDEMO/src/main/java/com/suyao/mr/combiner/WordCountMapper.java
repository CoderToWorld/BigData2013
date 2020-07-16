package com.suyao.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-17 20:44
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //定义输出的v
    IntWritable outV = new IntWritable(1);

    //定义输出的k
    Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1. 将输入的一行数据，转换成String类型.
        //  atguigu atguigu
        String line = value.toString();
        //2. 使用空格切分数据
        String[] splits = line.split(" ");
        //3. 迭代splits数组，将每个单词处理成kv，写出去.
        for (String word : splits) {
            //将当前单词设置到outK中
            outK.set(word);
            //写出
            context.write(outK,outV);
        }
    }
}
