package com.qcj.map_reduce._04mapreduce_sort_writable_test.customCounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义计数器:全局计数器
 * 定义一个枚举类：特殊的单例设计模式
 *     需求
 * 	统计学生成绩不合法字段
 *
 * 	  Counter接口中的主要方法:
 * 	     getName（）获取计数器的名字
 * 	     getValue()获取计数器的当前值
 * 	     setValue()为计数器设置值
 * 	     increment（）增加计数器的值 类似于sum+=value
 */
public class Counter_illegal_data {
    static class MyMapper extends Mapper<LongWritable, Text, NullWritable,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            //字段确实情况
            Counter counter = context.getCounter(customCounter.ILLEGAL_DATA_RECORDS);
            if(datas.length != 3){
                //获取计数器 进行++
                //对计数器+1
                counter.increment(1L);
            }else if(Integer.parseInt(datas[2]) < 0 || Integer.parseInt(datas[2]) > 100){//分数不合法
                counter.increment(1L);
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        configuration.set("fs.defaultFs","hdfs://hadoop1:9000");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(Counter_illegal_data.class);
            job.setMapperClass(MyMapper.class);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(NullWritable.class);


            Path inpath = new Path("hdfs://hadoop1:9000/counter_in");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/counter_out");
            FileOutputFormat.setOutputPath(job,outpath);

            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
