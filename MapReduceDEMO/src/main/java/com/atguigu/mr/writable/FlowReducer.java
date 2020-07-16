package com.atguigu.mr.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    private FlowBean  outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //总上行
        long  totalUpFlow = 0 ;
        //总下行
        long  totalDownFlow = 0 ;
        //总流量
        long totalSumFlow = 0 ;

        //处理一个手机号的总上行  总下行  总流量
        for (FlowBean flowBean : values) {
            totalUpFlow  += flowBean.getUpFlow();
            totalDownFlow +=flowBean.getDownFlow();
            totalSumFlow += flowBean.getSumFlow();
        }

        //封装value
        outV.setUpFlow(totalUpFlow);
        outV.setDownFlow(totalDownFlow);
        outV.setSumFlow(totalSumFlow);

        //写出
        context.write(key,outV);
    }
}
