package com.qcj.map_reduce._01mapreduce_group;

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
 *
 *      分组
 *
 * 在三题目的结果上 再按照
 * 平均分排序  科目分组 取前三
 */
public class StudentAvgDesc_group {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(StudentAvgDesc_group.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(StudentBean.class);
            job.setMapOutputValueClass(Text.class);
            //如果map输出的key value的类型和reduce输出的类型相同这里值设置reduce输出就行
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(StudentBean.class);

            //设定分组
            job.setGroupingComparatorClass(MyGroup.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/out_avg_score3");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_avg_score_sort");
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

    static class MyMapper extends Mapper<LongWritable, Text, StudentBean,Text> {
        Text mv = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            mv.set(datas[0]);
            StudentBean sb = new StudentBean(datas[1],Integer.parseInt(datas[2]));
            context.write(sb,mv);
        }
    }
    static class MyReducer extends Reducer<StudentBean,Text,Text, StudentBean> {
        @Override
        protected void reduce(StudentBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            System.out.println("-----------");
            for (Text v:values) {
                System.out.println(key+"----"+v);
                count++;
                if(count>3){
                    break;
                }
                context.write(v,key);
            }
            System.out.println("===============");
        }
    }


}
