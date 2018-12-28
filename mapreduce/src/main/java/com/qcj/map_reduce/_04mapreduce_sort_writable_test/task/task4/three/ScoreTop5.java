package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task4.three;

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
 * 3.求每个班级的总分最高的前5个学生
 *  分组：班级
 *  排序：总分
 */
public class ScoreTop5 {
    static class MyMapper extends Mapper<LongWritable, Text, ScoreBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            ScoreBean scoreBean = new ScoreBean(Integer.parseInt(datas[0].trim()),Integer.parseInt(datas[1].trim()),datas[2],Integer.parseInt(datas[3].trim()),Double.parseDouble(datas[4]));
            context.write(scoreBean,NullWritable.get());
        }
    }

    static class MyReducer extends Reducer<ScoreBean, NullWritable,ScoreBean, NullWritable> {
        @Override
        protected void reduce(ScoreBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (NullWritable v:values) {
                count ++;
                if(count > 5){
                    break;
                }
                context.write(key,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ScoreTop5.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
            //设置分组
            job.setGroupingComparatorClass(MyGroup.class);

            job.setMapOutputKeyClass(ScoreBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(ScoreBean.class);
            job.setOutputValueClass(NullWritable.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/day13_one");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/day13_three");
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
