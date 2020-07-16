package com.atguigu.mr.diyPartitioner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 对每个手机号的  上行  下行  总流量 的封装
 */
public class FlowBean implements Writable {

    private Long upFlow;   // 上行流量
    private Long downFlow; // 下行流量
    private Long sumFlow ; // 总流量

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow(){
        this.sumFlow = this.upFlow + this.downFlow;
    }

    /**
     * 重写toString方法
     */
    @Override
    public String toString() {
        return  this.upFlow + "\t" +this.downFlow +"\t" +this.sumFlow;
    }
    /**
     * 提供无参数构造器， 反序列时会反射调用
     */
    public FlowBean(){

    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow  = in.readLong();
        this.sumFlow = in.readLong();
    }
}
