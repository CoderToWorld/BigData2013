package com.suyao.mr.diyPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-17 18:28
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long totallUpFlow = 0;
        long totalDownFlow = 0;
        long totalSumFlow = 0;
        //迭代处理每一个手机号的总上行 下行 总流量
        for (FlowBean flowBean : values) {
            totallUpFlow += flowBean.getUpFlow();
            totalDownFlow += flowBean.getDownFlow();
            totalSumFlow += flowBean.getSumFlow();
        }
        //封装value
        outV.setUpFlow(totallUpFlow);
        outV.setDownFlow(totalDownFlow);
        outV.setSumFlow(totalSumFlow);

        //写出
        context.write(key,outV);
    }
}
