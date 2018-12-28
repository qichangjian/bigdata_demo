package com.qcj.map_reduce._04mapreduce_sort_writable_test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 科目升序    平均分 降序
 */
public class StudentBean implements WritableComparable<StudentBean> {
    private String subject;
    private int subjectAvg;

    public StudentBean() {
    }

    public StudentBean(String subject, int subjectAvg) {
        this.subject = subject;
        this.subjectAvg = subjectAvg;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getSubjectAvg() {
        return subjectAvg;
    }

    public void setSubjectAvg(int subjectAvg) {
        this.subjectAvg = subjectAvg;
    }

    @Override
    public String toString() {
        return subject + "\t" + subjectAvg;
    }

    /**
     * 注意：
     * 如果有double类型 需要手动做判断
     * 向上取整是不行的Math.ceil(double);负数取整就是0了
     * if(double < 0){
     *     temp=-1
     * }else if(double > 0){
     *     temp=1
     * }else{
     *     temp=0
     * }
     */
    public int compareTo(StudentBean o) {
        int result = this.getSubject().compareTo(o.getSubject());//科目升序
        if(result == 0){
            result = o.getSubjectAvg() - this.getSubjectAvg();
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(subject);
        dataOutput.writeInt(subjectAvg);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.subject = dataInput.readUTF();
        this.subjectAvg = dataInput.readInt();
    }
}
