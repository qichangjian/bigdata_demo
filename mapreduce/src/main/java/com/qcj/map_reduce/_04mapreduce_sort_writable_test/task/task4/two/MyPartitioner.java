package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task4.two;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义分区
 * 按照 班级分区
 */
public class MyPartitioner extends Partitioner<ScoreBean, NullWritable> {

    public int getPartition(ScoreBean key, NullWritable value, int num) {
        int result = 0;
        ScoreBean sb = key;

        Map<Integer,Integer> map= new HashMap<Integer,Integer>();
        map.put(1303,0);
        map.put(1304,1);
        map.put(1305,2);
        map.put(1306,3);
        map.put(1307,4);
        for (Map.Entry<Integer,Integer> entry:map.entrySet()) {
            if(entry.getKey() == sb.getClassName()){
                result = entry.getValue();
            }
        }
        return result;
    }
}
