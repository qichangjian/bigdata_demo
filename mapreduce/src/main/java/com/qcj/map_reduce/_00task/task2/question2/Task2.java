package com.qcj.map_reduce._00task.task2.question2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 第二问：每种商品销售额最多的三周
 */
public class Task2 {
    static class MyMapper extends Mapper<LongWritable, Text, GoodBean, NullWritable> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] datas = value.toString().split("\t");
            double sum = Double.parseDouble(datas[2]) * Double.parseDouble(datas[3]);
            GoodBean p = new GoodBean(datas[1], sum, datas[0]);
            context.write(p, NullWritable.get());

        }
    }

    public static class MyReducer extends Reducer<GoodBean, NullWritable, GoodBean, NullWritable> {
        @Override
        protected void reduce(GoodBean key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (NullWritable nv : values) {
                if (count < 3) {
                    context.write(key, nv);
                }
                count++;
            }
        }
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop1");
        Configuration cf = new Configuration();
        try {
            Job job = Job.getInstance(cf);
            job.setJarByClass(Task2.class);
            job.setMapperClass(MyMapper.class);

            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(GoodBean.class);
            job.setOutputValueClass(NullWritable.class);
            job.setGroupingComparatorClass(MyGroup.class);
            Path inpath = new Path("hdfs://hadoop1:9000/good_in");
            FileInputFormat.addInputPath(job, inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/good_out_3");
            FileOutputFormat.setOutputPath(job, outpath);
            job.waitForCompletion(true);

        } catch (Exception e) {

            e.printStackTrace();
        }
    }


}