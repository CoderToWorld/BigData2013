package com.suyao.mr.groupComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-17 21:32
 */
public class OrderMapper extends Mapper<LongWritable,Text,OrderBean, NullWritable> {
    private OrderBean outK = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        outK.setOrderId(splits[0]);
        outK.setPrice(Double.parseDouble(splits[2]));
        context.write(outK,NullWritable.get());

    }
}
