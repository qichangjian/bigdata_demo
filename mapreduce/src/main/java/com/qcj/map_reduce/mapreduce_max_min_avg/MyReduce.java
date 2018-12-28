package com.qcj.map_reduce.mapreduce_max_min_avg;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<Text, IntWritable,Text, DoubleWritable> {
    int max=0;
    int min=1000;
    int sum=0;
    int count=0;
    Text rk = new Text();
    DoubleWritable rv = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //循环遍历values
        for (IntWritable v:values) {
            int data = v.get();
            count++;
            sum+=data;
            //最大值
            if(max < data){
                max = data;
            }
            if(min > data){
                min = data;
            }
        }
        double avg = (double)sum/count;
        //输出
        rk.set("最大值");
        rv.set(max);
        context.write(rk,rv);//可以调用多次，调用一次就写出一次数据

        rk.set("最小值");
        rv.set(min);
        context.write(rk,rv);

        rk.set("平均值");
        rv.set(avg);
        context.write(rk,rv);
    }
}
