package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task5;

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
import java.util.ArrayList;
import java.util.List;

/**
 * 影评案例连接三表
 */
public class ReduceJoin_Yingping2 {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ReduceJoin_Yingping2.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            Path inpath = new Path("hdfs://hadoop1:9000/yingping_out_two");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/yingping_out_two2");
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

    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text mk = new Text();
        Text mv = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            if(datas.length == 2){
                String[] sdatas = datas[1].split("::");
                mk.set(sdatas[0]);
                String smv = "m"+sdatas[1] +","+sdatas[2];
                mv.set(smv);
                context.write(mk,mv);
            }else if(datas.length==8){
                mk.set(datas[6]);
                String smv = "t";
                for (int i = 0; i < datas.length; i++) {
                    smv += datas[i] + ",";
                }
                mv.set(smv.substring(0,smv.length()-1));
                context.write(mk,mv);
            }

        }
    }

    static class MyReducer extends Reducer<Text, Text,Text, NullWritable> {
        Text rk = new Text();
        //创建两个list数组
        List<String> twoList = new ArrayList<String>();
        List<String> moviesList = new ArrayList<String>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v:values) {
                String dt = v.toString();
                if(dt.startsWith("m")){
                    moviesList.add(dt.substring(1));
                }else if(dt.startsWith("t")){
                    twoList.add(dt.substring(1));
                }
            }

            for (String movies:moviesList) {
                for (String two:twoList) {
                    String res = two + "," + movies;
                    rk.set(res);
                    context.write(rk,NullWritable.get());
                }
            }
        }
    }
}
