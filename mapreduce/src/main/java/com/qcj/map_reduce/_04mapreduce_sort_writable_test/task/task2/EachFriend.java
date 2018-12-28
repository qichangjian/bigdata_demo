package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task2;

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
 * 互粉好友
 * 将A B 或者B A 都转换为 A:B
 * 如果reduce能接受到两个就是互粉好友
 */
public class EachFriend {
    static class MyMapper extends Mapper<LongWritable, Text,Text, Text> {
        Text mk = new Text();
        Text mv = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //A:B,C,D,F,E,O
            String[] datas = value.toString().split(":");
            String[] vdatas = datas[1].split(",");
            for (String s:vdatas) {
                mk.set(recombination(datas[0],s));
                mv.set(datas[0]);
                context.write(mk,mv);
            }
        }

        //将A B 或者B A 都转换为 A:B
        public String recombination(String a,String b){
            if(a.compareTo(b) < 0){
                return a + ":" + b;
            }else {
                return b + ":" + a;
            }
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,NullWritable> {
        Text mk = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            System.out.println("-------------------------");
            for (Text v:values) {
                System.out.println(key+"=="+v);
                count ++;
                if (count == 2){
                    context.write(key,NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(EachFriend.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/commonfriend_in");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/commonfriend_out_hufen");
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
