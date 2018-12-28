package com.qcj.map_reduce._03reducetask_bingxingdu.combiner_reducetask;

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

public class WordCount_Combiner {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(WordCount_Combiner.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            //指定添加combiner组件
            job.setCombinerClass(MyCombiner.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/wcin");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/wcout_combiner");
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

    static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");
            for (String w:words) {
                Text mk = new Text(w);
                IntWritable mv = new IntWritable(1);
                context.write(mk,mv);
            }
        }
    }

    static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v:values) {
                System.out.println(key+"-----------"+v.get());
                sum += v.get();
            }
            IntWritable rv = new IntWritable(sum);
            context.write(key,rv);
        }
    }
}
