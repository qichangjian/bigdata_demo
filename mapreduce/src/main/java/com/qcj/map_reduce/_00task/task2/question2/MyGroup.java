package com.qcj.map_reduce._00task.task2.question2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组：按照商品编号
 */
public class MyGroup extends WritableComparator {

    public MyGroup() {
        super(GoodBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        GoodBean sa = (GoodBean) a;
        GoodBean sb = (GoodBean) b;
        return sa.getSpbh().compareTo(sb.getSpbh());
    }
}