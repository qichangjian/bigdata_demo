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
import java.util.Arrays;

/**
 * 1、求所有两两用户之间的共同好友
 */
public class CommonFriend {
    static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text mk = new Text();
        Text mv = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //A:B,C,D,F,E,O
            String[] datas = value.toString().split(":");
            String[] vdatas = datas[1].split(",");
            for (String s:vdatas) {
                mk.set(s);
                mv.set(datas[0]);
                context.write(mk,mv);//B A（B是A的好友）
            }
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text mk = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text v:values) {
                sb.append(v).append("-");//有相同好友的人分在了一组 （B是哪些人的好友）
            }
            String[] s_values = sb.substring(0,sb.toString().length()-1).split("-");
            Arrays.sort(s_values);//给string数组排序
            for (int i = 0; i < s_values.length; i++) {
                for (int j = i+1; j < s_values.length; j++) {
                    String temp = s_values[i] + "-" + s_values[j];
                    mk.set(temp);
                    context.write(mk,key);//key拼接而成的两人都有好友values
                }
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(CommonFriend.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/commonfriend_in");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/commonfriend_in_one");
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
