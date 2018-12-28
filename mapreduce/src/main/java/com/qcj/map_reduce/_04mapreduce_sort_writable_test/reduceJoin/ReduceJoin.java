package com.qcj.map_reduce._04mapreduce_sort_writable_test.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
 *  Join
 *  两个文件中 具有相同字段 关联  但是相同字段不在同一个位置
 */
public class ReduceJoin {
    static class MyMapper extends Mapper<LongWritable,Text, Text,Text>{
        String filename = "";//文件路径
        Text mk = new Text();
        Text mv = new Text();
        /**
         *   获取文件名
         *
         * 一个maptask只调用一次
         * mapper -- 一个maptask -- 一个切片
         * @param context 上下文对象
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取文件的切片信息  InputSplit抽象类  FileSpilt实现类
            //两个文件两个切片
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            filename = fileSplit.getPath().toString();//获取文件路径
        }

        /**
         * t_product t_order
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            if(filename.endsWith("t_order")){
                mk.set(datas[2]);
                mv.set("o"+datas[0]+"\t"+datas[1]+"\t"+datas[3]);
                context.write(mk,mv);
            }else{
                mk.set(datas[0]);
                mv.set("p"+datas[1]+"\t"+datas[2]+"\t"+datas[3]);
                context.write(mk,mv);
            }
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text rv = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //创建两个list集合
            List<String> orderList = new ArrayList<String>();
            List<String> productList = new ArrayList<String>();
            System.out.println(key+"-----------------");
            //遍历values
            for (Text v:values) {
                String dt = v.toString();
                System.out.println(key+"======"+dt);
                if(dt.startsWith("o")){
                    orderList.add(dt.substring(1));
                }else {
                    productList.add(dt.substring(1));
                }
            }
            //将两个集合中的数据进行拼接
            for (String ol:orderList) {
                for (String pl:productList) {
                    String res = ol+"\t"+pl;
                    rv.set(res);
                    context.write(key,rv);
                }
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ReduceJoin.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/join_in");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/join_out");
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
