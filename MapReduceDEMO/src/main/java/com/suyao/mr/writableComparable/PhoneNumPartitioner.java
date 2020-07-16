package com.suyao.mr.writableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author suyso
 * @create 2020-04-17 19:49
 */
public class PhoneNumPartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        int partitioner;
        String phoneNum = text.toString();
        if (phoneNum.startsWith("136")) {
            partitioner = 0;
        } else if (phoneNum.startsWith("137")) {
            partitioner = 1;
        } else if (phoneNum.startsWith("138")) {
            partitioner = 2;
        } else if (phoneNum.startsWith("139")) {
            partitioner = 3;
        } else {
            partitioner = 4;
        }
        return partitioner;
    }
}
