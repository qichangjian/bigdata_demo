package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task1;

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
 * 1、求所有两两用户之间的共同好友
 */
public class CommonFriend2 {
    static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text mk = new Text();
        Text mv = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //F-I	A (FI都有A好友)
            String[] datas = value.toString().split("\t");
            mk.set(datas[0]);
            mv.set(datas[1]);
            context.write(mk,mv);
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text mv = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text v:values) {
                sb.append(v).append("-");
            }
            //把key两个人的共同好友value都凭借在一起  A-F	C-E-O-D-B（AF的共同好友是ceodb）
            mv.set(sb.substring(0,sb.toString().length()-1));
            context.write(key,mv);
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(CommonFriend2.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/commonfriend_in_one");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/commonfriend_in_two");
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
