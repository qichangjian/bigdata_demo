package com.qcj.map_reduce._00task.task2.question2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义类：
 *    商品编号： 分组
 *    商品总价：排序
 */
public class GoodBean implements WritableComparable<GoodBean> {
    private String spbh;//商品编号
    private Double spzj;//商品总价
    private String week;//周

    public String getWeek() {
        return week;
    }

    public void setWeek(String week) {
        this.week = week;
    }

    public String getSpbh() {
        return spbh;
    }

    public void setSpbh(String spbh) {
        this.spbh = spbh;
    }

    public Double getSpzj() {
        return spzj;
    }

    public void setSpzj(Double spzj) {
        this.spzj = spzj;
    }

    @Override
    public String toString() {
        return spbh + "\t" + week +"-"+ spzj;
    }

    public GoodBean() {
        super();
    }

    public GoodBean(String spbh, Double spzj, String week) {
        super();
        this.spbh = spbh;
        this.spzj = spzj;
        this.week = week;
    }

    public int compareTo(GoodBean o) {
        int tmp = this.getSpbh().compareTo(o.getSpbh());
        if (tmp == 0) {
            double tmp1 = o.getSpzj() - this.getSpzj();
            if (tmp1 < 0) {
                tmp = 1;
            } else if (tmp1 > 0) {
                tmp = -1;
            } else {
                tmp = 0;
            }

        }
        return tmp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(spbh);
        dataOutput.writeDouble(spzj);
        dataOutput.writeUTF(week);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.spbh = dataInput.readUTF();
        this.spzj = dataInput.readDouble();
        this.week = dataInput.readUTF();
    }

}

