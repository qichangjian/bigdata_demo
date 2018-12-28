package com.qcj.map_reduce._04mapreduce_sort_writable_test;

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
 * 1、统计每门课程的参考人数和课程平均分，
 * /in_avg_score
 * /out_avg_score
 */
public class CourseCountAndAVG {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(CourseCountAndAVG.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/in_avg_score");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_avg_score_2");
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

    static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text mk = new Text();
        Text mv = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            mk.set(datas[0]);
            StringBuilder name_score = new StringBuilder();
            for (int i = 1; i < datas.length; i++) {
                if(i==datas.length-1){
                    name_score.append(datas[i]);
                }else{
                    name_score.append(datas[i]).append(",");
                }
            }
            mv.set(name_score.toString());
            context.write(mk,mv);
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text rv = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int peopleNum = 0;
            int scoreAll=0;
            int scoreNum=0;
            for (Text v:values) {
                String[] onePeople = v.toString().split(",");
                peopleNum ++;
                for (int i = 1; i < onePeople.length; i++) {
                    int score = Integer.parseInt(onePeople[i]);
                    scoreAll += score;
                    scoreNum ++;
                }
            }
            rv.set(new StringBuilder().append(peopleNum).append("\t").append(scoreAll/scoreNum).toString());
            context.write(key,rv);
        }
    }
}
