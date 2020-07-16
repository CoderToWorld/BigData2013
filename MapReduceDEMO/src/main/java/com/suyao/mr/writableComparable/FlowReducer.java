package com.suyao.mr.writableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-17 19:48
 */
public class FlowReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //注意一个问题: 不同的手机号但是总流量相同的情况，会成为一组kv ，进入一个reduce方法。
        //            手机号相同，但是总流量不同的情况，会被分开处理。

        //直接迭代values，将每个手机号的数据直接写出
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
