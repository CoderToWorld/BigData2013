package com.suyao.mr.groupComparator2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    private OrderBean outK = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //一行数据
        // 10000001	Pdt_01	222.8
        String line = value.toString();
        String[] splits = line.split("\t");

        //封装k
        outK.setOrderId(splits[0]);
        outK.setPrice(Double.parseDouble(splits[2]));

        //写出
        context.write(outK,NullWritable.get());
    }
}
