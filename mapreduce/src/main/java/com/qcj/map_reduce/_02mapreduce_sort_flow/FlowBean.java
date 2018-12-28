package com.qcj.map_reduce._02mapreduce_sort_flow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  自定义排序：能排序，能传输
 *  泛型：用于比较的对象的类型
 *
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private String phoneNum;//手机号
    private int upFlow;//上行流量
    private int downFlow;//下行流量
    private int sumFlow;//总流量

    public FlowBean() {
    }

    public FlowBean(String phoneNum, int upFlow, int downFlow, int sumFlow) {
        this.phoneNum = phoneNum;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

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

    @Override
    public String toString() {
        return phoneNum + '\t' + upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }

    //比较：先比较总流量倒叙，再比较手机号升序
    public int compareTo(FlowBean o) {
        int tmp = o.getSumFlow() - this.getSumFlow();
        if(tmp == 0){
            tmp = this.getPhoneNum().compareTo(o.getPhoneNum());
        }
        return tmp;
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNum);
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(sumFlow);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.phoneNum = dataInput.readUTF();
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.sumFlow = dataInput.readInt();
    }
}
