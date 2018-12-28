package com.qcj.map_reduce.mapreduce_writable_flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义类：上行流量 + 下行流量
 *        实现Writable接口重写方法
 */
public class FlowBean implements Writable {
    private int upFlow;//上行流量
    private int downFlow;//下行流量
    private int sumFlow;//总流量

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(int sumFlow) {
        this.sumFlow = sumFlow;
    }

    public FlowBean(int upFlow, int downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public FlowBean() {
    }

    @Override
    public String toString() {
        return upFlow +
                "" +
                "\t" + downFlow +
                "\t" + sumFlow;
    }

    //序列化的方法  原始数据 转换为 二进制数据
    public void write(DataOutput dataOutput) throws IOException {
        //通过参数的输出流写出
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(sumFlow);
        //字符串序列化用writeUTF
    }

    //反序列化方法    二进制数据  转换为  原始数据
    public void readFields(DataInput dataInput) throws IOException {
        //从流中读取数据 转换为原始数据
        //注意：反序列化的顺序一定要和序列化的数据一致
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.sumFlow = dataInput.readInt();
    }
}
