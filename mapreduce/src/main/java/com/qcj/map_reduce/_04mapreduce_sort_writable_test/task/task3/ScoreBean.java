package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义类
 *
 * 课程 姓名 平均值
 */
public class ScoreBean implements WritableComparable<ScoreBean> {
    private String scoreName;
    private String studentName;
    private double scoreAvg;

    public ScoreBean() {
    }

    public ScoreBean(String scoreName, String studentName, double scoreAvg) {
        this.scoreName = scoreName;
        this.studentName = studentName;
        this.scoreAvg = scoreAvg;
    }

    public String getScoreName() {
        return scoreName;
    }

    public void setScoreName(String scoreName) {
        this.scoreName = scoreName;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public double getScoreAvg() {
        return scoreAvg;
    }

    public void setScoreAvg(double scoreAvg) {
        this.scoreAvg = scoreAvg;
    }

    @Override
    public String toString() {
        return scoreName + "\t" + studentName + "\t" + scoreAvg;
    }

    //课程分组 平均分排序
    public int compareTo(ScoreBean o) {
        int tmp = this.scoreName.compareTo(o.scoreName);
        double dtmep = o.scoreAvg - this.scoreAvg;
        if(dtmep > 0){
            return 1;
        }else if(dtmep < 0){
            return -1;
        }else {
            return 0;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(scoreName);
        dataOutput.writeUTF(studentName);
        dataOutput.writeDouble(scoreAvg);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.scoreName = dataInput.readUTF();
        this.studentName = dataInput.readUTF();
        this.scoreAvg = dataInput.readDouble();
    }
}
