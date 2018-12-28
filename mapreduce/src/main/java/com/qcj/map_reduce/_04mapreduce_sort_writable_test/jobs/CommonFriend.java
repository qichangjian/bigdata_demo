package com.qcj.map_reduce._04mapreduce_sort_writable_test.jobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * 1、求所有两两用户之间的共同好友
 */
public class CommonFriend {
    static class MyMapper1 extends Mapper<LongWritable, Text,Text,Text>{
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

    static class MyReducer1 extends Reducer<Text,Text,Text,Text>{
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
}
