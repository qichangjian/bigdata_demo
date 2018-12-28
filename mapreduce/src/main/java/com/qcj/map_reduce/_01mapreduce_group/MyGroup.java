package com.qcj.map_reduce._01mapreduce_group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组
 *    分组是在排序的基础上
 *    分组的时候只判断相邻的
 */
class MyGroup extends WritableComparator {
    //写构造方法 调用父类的三个参数的 最后一个参数改为true
    /**
     *  默认它不会给你创建用于比较的对象   如果不写它会报错：空指针异常
     *
     * param keyClass  比较的对象类型的class  StudentBean.class
     * param conf      配置文件对象
     * param createInstances  是否创建对象
     */
    public MyGroup() {
        //自动创建需要的比较对象
        super(StudentBean.class, true);
    }

    /**
     * 重写方法
     * 两个参数代表两个比较对象（Map的key）
     * @param a
     * @param b
     * @return 相邻的比较为0 一组，  不为0的不是一组
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //按照课程进行分组
        StudentBean asb = (StudentBean)a;
        StudentBean bsb = (StudentBean)b;
        return asb.getSubject().compareTo(bsb.getSubject());//按照课程进行分组
    }
}
