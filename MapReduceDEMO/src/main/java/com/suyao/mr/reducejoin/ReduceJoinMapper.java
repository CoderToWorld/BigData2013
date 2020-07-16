package com.suyao.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author suyso
 * @create 2020-04-19 14:19
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
    private String currentSplitFileName;

    private OrderBean outV = new OrderBean();
    private Text outK = new Text();

    /**
     * 在MapTask执行开始时执行一次
     * 获取当前处理的切片对应的文件是哪个
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片信息
        InputSplit inputSplit = context.getInputSplit();
        //转换为FileSplit
        FileSplit currentSplit = (FileSplit) inputSplit;
        //获取当前处理的文件名
        currentSplitFileName = currentSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //处理一条数据
        String line = value.toString();
        String[] split = line.split("\t");

        //获取数据来自哪个文件
        if (currentSplitFileName.contains("order")) {
            //数据来自order.txt
            //1001  01 1
            //封装key
            outK.set(split[1]);
            //封装value
            outV.setOrderId(split[0]);
            outV.setPid(split[1]);
            outV.setAmout(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        } else {
            //数据来自pd.txt
            //01 小米
            //封装key
            outK.set(split[0]);
            //封装value
            outV.setPid(split[0]);
            outV.setPname(split[1]);
            outV.setOrderId("");
            outV.setAmout(0);
            outV.setFlag("pd");
        }
        //写出
        context.write(outK, outV);
    }
}
