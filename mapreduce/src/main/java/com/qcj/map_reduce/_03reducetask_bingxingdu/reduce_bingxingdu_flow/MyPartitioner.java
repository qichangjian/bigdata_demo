package com.qcj.map_reduce._03reducetask_bingxingdu.reduce_bingxingdu_flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 泛型指的是map输出的key和value
 *
 *  分区算法决定了map输出的数据 如何分配给reduce的
 */
public class MyPartitioner extends Partitioner<Text,Text> {
    /**
     * @param key               map输出的key
     * @param value             map输出的value
     * @param numPartitions     分区的个数
     * @return
     *
     * 分区规则：key是
     * 134,135,136  ---------bj
     * 137 138 139  ---------sh
     * other        ---------sz
     *
     * 注意：设定分区编号的时候，最好顺序递增，不要跳数
     *
     * 如果分区设置为n   可以设置 1  n  >n 个 reducetask个数
     *      reducetask个数为1：默认全部到一个reducetask中
     *      reducetask个数>1：按照分区编号，对应的分区到对应的reducetask中
     *
     *  所以理论上：设置的时候reducetask个数一定要等于分区的个数。
     *
     */
    public int getPartition(Text key, Text value, int numPartitions) {
        //分区可以使用map(手机号的前三位) value（分区编号）
        String mk = key.toString();
        if(mk.startsWith("134")||mk.startsWith("135")||mk.startsWith("136")){
            return 0;//返回第一个分区
        }else if(mk.startsWith("137")||mk.startsWith("138")||mk.startsWith("139")){
            return 1;
        }else{
            return 2;//分区编号最大值不能大于设置的maptask个数 不然会报错，错误分区 illegal partition
        }
    }
}
