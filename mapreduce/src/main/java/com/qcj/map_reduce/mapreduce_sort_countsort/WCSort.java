package com.qcj.map_reduce.mapreduce_sort_countsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 将WordCount的结果进行按照次数排序(降序  升序 )
 */
public class WCSort {
    //mapper类
    static class MyMapper extends Mapper<LongWritable, Text,IntWritable,Text> {
        Text mv = new Text();
        IntWritable mk = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] word_count = value.toString().split("\t");
            //mk.set(Integer.parseInt(word_count[1]));//升序排序
            mk.set(-Integer.parseInt(word_count[1]));//倒叙排序
            mv.set(word_count[0]);
            context.write(mk,mv);
        }
    }
    /**
     * reducer类
     * mapreduce程序中 输出的key 和 values中分割的是\t
     * */
    static class MyReducer extends Reducer<IntWritable,Text,Text,IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //相同的次数在一组中，values需要循环遍历
            for (Text v:values) {
                //context.write(v,key);//升序排序
                context.write(v,new IntWritable(-key.get()));//倒叙排序
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(WCSort.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/wc_out");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/wc_out_sort4");
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
