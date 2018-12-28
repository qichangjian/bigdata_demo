package com.qcj.map_reduce._04mapreduce_sort_writable_test.customInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 小文件合并
 * map将每一个小文件的内容发送给reduce, 在reduce合并
 */
public class MergeSmallFile {
    static class MyMapper extends Mapper<NullWritable,Text,NullWritable,Text> {
        //一个文件切片调用一次
        @Override
        protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(),value);
        }
    }

    static class MyReducer extends Reducer<NullWritable, Text,NullWritable,Text>{
        //一组调用一次
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v:values) {
                context.write(NullWritable.get(),v);
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        configuration.set("fs.defaultFs","hdfs://hadoop1:9000");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(MergeSmallFile.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            //设置job的输入为自定义输入
            job.setInputFormatClass(MyFileInputFormat.class);

            Path inpath = new Path("hdfs://hadoop1:9000/wcin");
            //用自定义的输入，不改也行
            MyFileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/merge_wc_in_out");
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
