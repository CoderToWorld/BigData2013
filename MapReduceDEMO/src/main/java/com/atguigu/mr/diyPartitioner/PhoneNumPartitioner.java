package com.atguigu.mr.diyPartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author suyso
 * @create 2020-04-17 10:19
 */
public class PhoneNumPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int partitioner;
        String key = text.toString();
        if (key.startsWith("136")) {
            partitioner = 0;
        } else if (key.startsWith("137")) {
            partitioner = 1;
        } else if (key.startsWith("138")) {
            partitioner = 2;
        } else if (key.startsWith("139")) {
            partitioner = 3;
        } else {
            partitioner = 4;
        }
        return partitioner;
    }
}
