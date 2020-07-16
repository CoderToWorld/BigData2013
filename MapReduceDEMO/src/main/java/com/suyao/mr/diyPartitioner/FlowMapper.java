package com.suyao.mr.diyPartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-17 18:28
 */

/**
 * 继承mapper，重写map方法
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    private Text outK = new Text();
    private FlowBean  outV = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //处理一行数据
        // 1   13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String line = value.toString();

        //切割数据
        String[] splits = line.split("\t");

        //封装key
        outK.set(splits[1]);

        //封装value
        outV.setUpFlow(Long.parseLong(splits[splits.length - 3]));
        outV.setDownFlow(Long.parseLong(splits[splits.length - 2]));
        outV.setSumFlow();

        //写出
        context.write(outK,outV);

    }
}
