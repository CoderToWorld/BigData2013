package com.suyao.mr.reducejoin;

/**
 * @author suyso
 * @create 2020-04-18 14:33
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装order.txt 及pd.txt中字段的信息
 */
public class OrderBean implements Writable {
    private String orderId;//订单id
    private String pid;//商品id
    private Integer amout;//购买数量
    private String pname;//商品名字
    private String flag;//用于标记数据来自于哪个文件

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Integer getAmout() {
        return amout;
    }

    public void setAmout(Integer amout) {
        this.amout = amout;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return this.orderId + "\t" + this.pname + "\t" + this.amout;
    }

    public OrderBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pid);
        out.writeInt(amout);
        out.writeUTF(pname);
        out.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.pid = in.readUTF();
        this.amout = in.readInt();
        this.pname = in.readUTF();
        this.flag = in.readUTF();

    }
}
