package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task4.one;

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
 * 1.求每个学生的总分和平均分，并按总分降序排序
 */
public class StudentAvgSumSort {
    static class MyMapper extends Mapper<LongWritable, Text, ScoreBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            int count = 0;
            int sumScore = 0;
            for (int i = 3; i < datas.length; i++) {
                count ++;
                sumScore += Integer.parseInt(datas[i].trim());
            }
            double avg = (sumScore*1.0)/count;
            ScoreBean scoreBean = new ScoreBean(Integer.parseInt(datas[0].trim()),Integer.parseInt(datas[1].trim()),datas[2],sumScore,avg);
            context.write(scoreBean,NullWritable.get());
        }
    }

    static class MyReducer extends Reducer<ScoreBean, NullWritable, ScoreBean, NullWritable>{
        @Override
        protected void reduce(ScoreBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(StudentAvgSumSort.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(ScoreBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(ScoreBean.class);
            job.setOutputValueClass(NullWritable.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/day13");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/day13_one");
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
