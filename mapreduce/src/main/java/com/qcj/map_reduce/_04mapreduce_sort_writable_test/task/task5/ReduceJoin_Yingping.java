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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 影评案例连接三表
 */
public class ReduceJoin_Yingping {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ReduceJoin_Yingping.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            Path inpath = new Path("hdfs://hadoop1:9000/yingping_in");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/yingping_out_two");
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
        String filename = "";//文件路径
        Text mk = new Text();
        Text mv = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            filename = fileSplit.getPath().toString();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("::");
            if(filename.endsWith("users.dat")){
                mk.set(datas[0]);
                String smv = "";
                for (int i = 1; i < datas.length; i++) {
                    smv += datas[i] + ",";
                }
                mv.set("u"+smv.substring(0,smv.length()-1));
                context.write(mk,mv);
            }else if(filename.endsWith("ratings.dat")){
                mk.set(datas[0]);
                String smv = "";
                for (int i = 1; i < datas.length; i++) {
                    smv += datas[i] + ",";
                }
                mv.set("r" + smv.substring(0,smv.length()-1));
                context.write(mk,mv);
            }else{
                mk.set("movies.dat");
                context.write(mk,new Text("m"+value.toString()));
            }

        }
    }

    static class MyReducer extends Reducer<Text, Text,Text, NullWritable> {
        Text rk = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //创建两个list数组
            List<String> userList = new ArrayList<String>();
            List<String> ratingsList = new ArrayList<String>();
            List<String> moviesList = new ArrayList<String>();

            for (Text v:values) {
                String dt = v.toString();
                if(dt.startsWith("u")){
                    userList.add(dt.substring(1));
                }else if(dt.startsWith("r")){
                    ratingsList.add(dt.substring(1));
                }else if(dt.startsWith("m")){
                    moviesList.add(dt.substring(1));
                }
            }

            //拼接前两个
            for (String user:userList) {
                for (String ratings:ratingsList) {
                    String res = key.toString() + "," + user + "," + ratings;
                    rk.set(res);
                    context.write(rk,NullWritable.get());
                }
            }
            //加入第三个
            for (String movies:moviesList) {
                context.write(new Text("movies,"+movies),NullWritable.get());
            }
        }
    }
}
