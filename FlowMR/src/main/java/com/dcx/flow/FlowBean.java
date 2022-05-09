package com.dcx.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 党楚翔 on 2022/5/9.
 */
public class FlowBean implements Writable {

    private long upFlow;

    private long downFlow;

    private long sumFlow;

    public FlowBean() {
    }

    public void set(long upFlow, long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    /**
     * 序列化方法
     * @param out  框架给我们提供的数据出口
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * @param in 框架提供的数据来源
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        //顺序：怎么序列化的顺序就应该怎么反序列化的顺序
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }


}