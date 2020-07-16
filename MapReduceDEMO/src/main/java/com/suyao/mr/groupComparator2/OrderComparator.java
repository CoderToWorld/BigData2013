package com.suyao.mr.groupComparator2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * OrderBean对象的分组比较器
 */
public class OrderComparator extends WritableComparator {

    public OrderComparator(){
        super(OrderBean.class,true);
    }

    /**
     * 比较规则: 只比较订单id
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        return aBean.getOrderId().compareTo(bBean.getOrderId());
    }
}
