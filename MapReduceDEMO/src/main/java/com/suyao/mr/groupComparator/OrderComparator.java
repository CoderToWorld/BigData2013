package com.suyao.mr.groupComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author suyso
 * @create 2020-04-17 21:51
 */
public class OrderComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        return aBean.getOrderId().compareTo(bBean.getOrderId());
    }
}
