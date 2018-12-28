package com.qcj.map_reduce.mapreduce_max_min_avg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 求最大值，最小值，平均值
 * 思路：将他们放在一组中
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text mk = new Text();
    IntWritable mv = new IntWritable();

    //一行调用一次，对象的创建放在外边
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将Text类型转换为String类型
        String line = value.toString();
        //2.切分
        String[] words = line.split("\t");
        //3.遍历
        for (String w:words) {
            //将类型转化为序列化类型
            //string -- text
            mk.set("a");
            //int -- intwritable
            mv.set(Integer.parseInt(w));
            //这里的write是直接写出，调用一次，写出一个到reduce端
            context.write(mk,mv);
        }
    }
}
