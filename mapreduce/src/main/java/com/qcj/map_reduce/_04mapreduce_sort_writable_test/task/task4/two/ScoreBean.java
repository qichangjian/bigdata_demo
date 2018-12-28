package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task4.two;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义类
 */
public class ScoreBean implements WritableComparable<ScoreBean> {
    private int className;
    private int studentNo;
    private String studentName;
    private int sumScore;
    private double avgScore;

    public ScoreBean() {
    }

    public ScoreBean(int className, int studentNo, String studentName, int sumScore, double avgScore) {
        this.className = className;
        this.studentNo = studentNo;
        this.studentName = studentName;
        this.sumScore = sumScore;
        this.avgScore = avgScore;
    }

    public int getClassName() {
        return className;
    }

    public void setClassName(int className) {
        this.className = className;
    }

    public int getStudentNo() {
        return studentNo;
    }

    public void setStudentNo(int studentNo) {
        this.studentNo = studentNo;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public int getSumScore() {
        return sumScore;
    }

    public void setSumScore(int sumScore) {
        this.sumScore = sumScore;
    }

    public double getAvgScore() {
        return avgScore;
    }

    public void setAvgScore(double avgScore) {
        this.avgScore = avgScore;
    }

    @Override
    public String toString() {
        return className + "\t" + studentNo + "\t" + studentName + "\t" + sumScore + "\t" + avgScore;
    }

    //课程分组
    public int compareTo(ScoreBean o) {
        return o.studentNo - this.studentNo;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(className);
        dataOutput.writeInt(studentNo);
        dataOutput.writeUTF(studentName);
        dataOutput.writeInt(sumScore);
        dataOutput.writeDouble(avgScore);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.className = dataInput.readInt();
        this.studentNo = dataInput.readInt();
        this.studentName = dataInput.readUTF();
        this.sumScore = dataInput.readInt();
        this.avgScore = dataInput.readDouble();
    }
}
