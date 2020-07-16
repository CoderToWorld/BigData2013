package com.suyao.mr.groupComparator2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;

    private Double price ;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String toString(){
        return orderId + "\t" + price ;
    }

    /**
     * 比较规则: 按照订单id升序， 价格降序
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
       return  this.orderId.compareTo(o.getOrderId())==0 ? -this.price.compareTo(o.getPrice()):this.orderId.compareTo(o.getOrderId());
    }

    public OrderBean(){}
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId  = in.readUTF();
        price = in.readDouble();
    }
}
