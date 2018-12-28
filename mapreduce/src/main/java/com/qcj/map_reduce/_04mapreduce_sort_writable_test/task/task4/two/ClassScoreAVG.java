package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task4.two;

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
 * 2.求每个班级每一门课程的平均分，不同班级的结果输出到不同的结果文件
 *
 *     分组：班级
 */
public class ClassScoreAVG {
    static class MyMapper extends Mapper<LongWritable, Text, ScoreBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            ScoreBean scoreBean = new ScoreBean(Integer.parseInt(datas[0].trim()),Integer.parseInt(datas[1].trim()),datas[2],Integer.parseInt(datas[3].trim()),Double.parseDouble(datas[4]));
            context.write(scoreBean,NullWritable.get());
        }
    }

    static class MyReducer extends Reducer<ScoreBean, NullWritable,Text, NullWritable> {
        @Override
        protected void reduce(ScoreBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sumAvg = 0;
            for (NullWritable v:values) {
                sumAvg += key.getAvgScore();
                count++;
            }
            double resultAvg = sumAvg/count;
            String rk = key.getClassName()+"\t"+key.getStudentNo()+"\t"+resultAvg;
            context.write(new Text(rk),NullWritable.get());
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ClassScoreAVG.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
            //设置分区
            job.setPartitionerClass(MyPartitioner.class);
            job.setNumReduceTasks(5);

            job.setMapOutputKeyClass(ScoreBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/day13_one");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/day13_two");
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
