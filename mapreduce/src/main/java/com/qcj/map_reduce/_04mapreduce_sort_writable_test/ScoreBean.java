package com.qcj.map_reduce._04mapreduce_sort_writable_test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用于第二题的排序
 */
public class ScoreBean implements WritableComparable<ScoreBean> {
    private int peopleNum;
    private int scoreAvg;

    public ScoreBean() {
    }

    public ScoreBean(int peopleNum, int scoreAvg) {
        this.peopleNum = peopleNum;
        this.scoreAvg = scoreAvg;
    }

    public int getPeopleNum() {
        return peopleNum;
    }

    public void setPeopleNum(int peopleNum) {
        this.peopleNum = peopleNum;
    }

    public int getScoreAvg() {
        return scoreAvg;
    }

    public void setScoreAvg(int scoreAvg) {
        this.scoreAvg = scoreAvg;
    }

    @Override
    public String toString() {
        return peopleNum + "\t" + scoreAvg;
    }

    public int compareTo(ScoreBean o) {
        return o.getScoreAvg() - this.getScoreAvg();//平均值降序
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(peopleNum);
        dataOutput.writeInt(scoreAvg);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.peopleNum = dataInput.readInt();
        this.scoreAvg = dataInput.readInt();
    }
}
