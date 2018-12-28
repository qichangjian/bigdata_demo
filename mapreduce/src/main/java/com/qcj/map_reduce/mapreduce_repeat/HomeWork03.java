package com.qcj.map_reduce.mapreduce_repeat;

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

public class HomeWork03 {


    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(HomeWork03.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReduce.class);

            //设置类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            //输入输出路径
            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/out_remove_nojar");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_remove_nojar_count");
            FileOutputFormat.setOutputPath(job,outpath);

            //提交
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
            if(datas.length == 3){
                String mkk = datas[0]+"\t"+datas[2];
                String mvv = datas[1];
                mk.set(mkk);
                mv.set(mvv);
                context.write(mk,mv);
            }
        }
    }

    static class MyReduce extends Reducer<Text,Text,Text,Text> {
        Text vout = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            StringBuilder stringBuilder = new StringBuilder();
            //values只能遍历一次，因为底层是指针的操作
            for (Text v:values) {
                count ++;
                stringBuilder.append(v.toString()).append(",");
            }
            String svalue = count + "\t" + stringBuilder.toString().substring(0,stringBuilder.length()-1);//去掉最后的，
            vout.set(svalue);
            context.write(key,vout);
        }
    }
}
