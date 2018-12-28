package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义分区
 *
 * 分区算法决定了map输出的数据 如何分配给reduce的
 */
public class MyPartitioner extends Partitioner<ScoreBean, NullWritable> {
    /**
     * @param key map key
     * @param value
     * @param numPartitions
     */
    public int getPartition(ScoreBean key, NullWritable value, int numPartitions) {
        int result = 0;
        ScoreBean mk = key;
        Map<String,Integer> map= new HashMap<String,Integer>();
        map.put("algorithm",0);
        map.put("computer",1);
        map.put("english",2);
        map.put("math",3);

        for (Map.Entry<String,Integer> entry : map.entrySet()) {
            if(entry.getKey().equals(mk.getScoreName())){
                result = entry.getValue();
            }
        }
        return result;
    }
}
