package com.qcj.map_reduce._03reducetask_bingxingdu.reduce_bingxingdu_flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *  将流量汇总统计结果按照手机归属地不同省份输出到不同文件中
 */
public class FlowPatition {
    static class MyMapper extends Mapper<LongWritable, Text,Text,Text> {
        Text mk = new Text();
        Text mv = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t",2);//分两份
            mk.set(datas[0]);
            mv.set(datas[1]);
            context.write(mk,mv);
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        //按照手机号分组的：没有重复的手机号
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v:values) {
                context.write(key,v);
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(FlowPatition.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            //指定自定义分区
            job.setPartitionerClass(MyPartitioner.class);
            //指定reducetask个数
            //启动三个reduce:一个reducetask会对应一个结果文件
            job.setNumReduceTasks(3);//这是reducetask的并行度为3

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/out_flow_bean_sort");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_flow_bean_sort_reduce");
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
