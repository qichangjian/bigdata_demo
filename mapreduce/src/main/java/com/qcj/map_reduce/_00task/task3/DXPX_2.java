package com.qcj.map_reduce._00task.task3;

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
import java.util.StringTokenizer;

/**
 *
 * 题目二：编写MapReduce程序求出每个关键词在每个文档出现了多少次，并且按照出现次数降序排序
 */
public class DXPX_2 {
    /**
     * map过程
     */
    static class MyMapper extends Mapper<LongWritable, Text,Text,Text> {
        Text mk = new Text();
        Text mv = new Text();
        FileSplit split;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit)context.getInputSplit();
            //StringTokenizer 只是使用逐字分裂的字符
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()){
                System.out.println("split.getPath().toString():-------"+split.getPath().toString());
                //凭借key为    字符串：URI
                String path = split.getPath().toString();
                mk.set(stringTokenizer.nextToken() + ":" + path.substring(path.length()-5));
                mv.set("1");//设置出现的次数
                context.write(mk,mv);
            }
        }
    }

    /**
     * Combine过程：完成词频统计
     *     通过一个Reduce过程无法同时完成词频统计和生成文档列表，
     *     所以必须增加一个Combine过程完成词频统计
     * 将key值相同的value值累加，得到一个单词在文档中的词频
     */
    static class MyCombiner extends Reducer<Text,Text,Text,Text>{
        Text ck = new Text();
        Text cv = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;//计数
            for (Text v:values) {
                sum += Integer.parseInt(v.toString());
            }
            String[] datas = key.toString().split(":");
            //设置value  URI:词频
            cv.set(datas[1] + "," + sum);
            //key  单词
            ck.set(datas[0]);
            context.write(ck,cv);
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text mv = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //生成文档列表  相同的单词 一条或者多条放在一起
            String res = new String();
            for (Text value:values) {
                res += value.toString() + ";";
            }
            mv.set(res.substring(0,res.length()-1));
            context.write(key,mv);
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(DXPX_2.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
            job.setCombinerClass(MyCombiner.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            Path inpath = new Path("hdfs://hadoop1:9000/dxpx_task");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/dxpx_task_out");
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
