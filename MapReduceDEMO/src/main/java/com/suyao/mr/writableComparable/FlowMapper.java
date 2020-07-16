package com.suyao.mr.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-17 19:48
 */
public class FlowMapper extends Mapper<LongWritable,Text,FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        outV.set(splits[1]);

        outK.setUpFlow(Long.parseLong(splits[splits.length - 3]));
        outK.setDownFlow(Long.parseLong(splits[splits.length - 2]));
        outK.setSumFlow();

        context.write(outK,outV);

    }
}
