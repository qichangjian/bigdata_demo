package com.qcj.map_reduce._00task.task2.question1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * 第二题：MapReduce题--基础复习
 *
   第一问：每种商品的销售总金额，并降序排序
 *
 * 两个job任务
 *      第一个job任务:  生成样式   商品 ： 总销量  但不是排序的
 *      第二个job任务： 按照总销量降序排序
 */
public class Task1 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        Text mk = new Text();
        DoubleWritable mv = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            mk.set(datas[1]);
            Double dmv = Double.parseDouble(datas[2]) * Double.parseDouble(datas[3]);
            mv.set(dmv);
            context.write(mk, mv);
        }
    }

    static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        DoubleWritable rv = new DoubleWritable();

        protected void reduce(Text key, Iterable<DoubleWritable> value,
                              Context context) throws IOException, InterruptedException {
            Double sum = 0D;
            for (DoubleWritable v : value) {
                sum += v.get();
            }
            rv.set(sum);
            context.write(key, rv);
        }
    }


    /**
     * 第二个job任务
     */
    static class MyMapper2 extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        DoubleWritable mk = new DoubleWritable();
        Text mv = new Text();

        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            Double dmk2 = Double.parseDouble(datas[1]);
            mk.set(-dmk2);
            mv.set(datas[0]);
            context.write(mk, mv);
        }
    }

    static class MyReducer2 extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        protected void reduce(DoubleWritable key, Iterable<Text> value,
                              Context context) throws IOException, InterruptedException {
            for (Text v:value) {
                context.write(v, new DoubleWritable(-key.get()));
            }
        }
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop1");
        Configuration cf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), cf, "hadoop1");
            Job job = Job.getInstance(cf);
            job.setJarByClass(Task1.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            Path inpath = new Path("hdfs://hadoop1:9000/good_in");
            Path outpath = new Path("hdfs://hadoop1:9000/good_out_1");
            //删除存在的目录
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileInputFormat.addInputPath(job, inpath);
            FileOutputFormat.setOutputPath(job, outpath);

            //第二个job任务
            Job job2 = Job.getInstance(cf);
            job2.setJarByClass(Task1.class);

            job2.setMapperClass(MyMapper2.class);
            job2.setReducerClass(MyReducer2.class);
            job2.setMapOutputKeyClass(DoubleWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            Path inputPath2 = new Path("hdfs://hadoop1:9000/good_out_1");
            Path outputPath2 = new Path("hdfs://hadoop1:9000/good_out_2");
            if (fs.exists(outputPath2)) {
                fs.delete(outputPath2, true);
            }

            FileInputFormat.setInputPaths(job2, inputPath2);
            FileOutputFormat.setOutputPath(job2, outputPath2);

            JobControl control = new JobControl("ss");

            ControlledJob conjob1 = new ControlledJob(job.getConfiguration());
            ControlledJob conjob2 = new ControlledJob(job2.getConfiguration());

            conjob2.addDependingJob(conjob1);

            control.addJob(conjob1);
            control.addJob(conjob2);

            Thread t = new Thread(control);
            t.start();

            while (!control.allFinished()) {
                Thread.sleep(1000);
            }

            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
