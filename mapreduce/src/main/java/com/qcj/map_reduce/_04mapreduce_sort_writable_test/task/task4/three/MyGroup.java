package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task4.three;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组
 * 班级
 */
public class MyGroup extends WritableComparator {
    public MyGroup() {
        super(ScoreBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ScoreBean asb = (ScoreBean)a;
        ScoreBean bsb = (ScoreBean)b;

        return asb.getClassName() - bsb.getClassName();
    }
}
