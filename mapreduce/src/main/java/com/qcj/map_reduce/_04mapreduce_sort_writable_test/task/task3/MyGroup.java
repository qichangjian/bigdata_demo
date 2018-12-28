package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组
 * 按照课程分组
 */
public class MyGroup extends WritableComparator {

    public MyGroup() {
        //自动构建需要比较的对象
        super(ScoreBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ScoreBean asb = (ScoreBean)a;
        ScoreBean bsb = (ScoreBean)b;
        return asb.getScoreName().compareTo(bsb.getScoreName());
    }
}
