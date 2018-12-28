package com.qcj.map_reduce._00task.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 倒叙排序
 * 题目一：编写MapReduce求出以下格式的结果数据：统计每个关键词在每个文档中当中的第几行出现了多少次
 * 例如，huangxiaoming关键词的格式：
 * huangixaoming	mapreduce-4-1.txt:2,2; mapreduce-4-1.txt:4,1;mapreduce-4-2.txt:3,1
 * 以上答案的意义：
 * 关键词huangxiaoming在第一份文档mapreduce-4-1.txt中的第2行出现了2次
 * 关键词huangxiaoming在第一份文档mapreduce-4-1.txt中的第4行出现了1次
 * 关键词huangxiaoming在第二份文档mapreduce-4-2.txt中的第3行出现了1次
 */
public class DXPX {
    private static int count1 =0;
    private static int count2 =0;
    /**
     * map过程
     */
    static class MyMapper extends Mapper<LongWritable, Text,Text,Text> {
        Text mk = new Text();
        Text mv = new Text();
        String filename="";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //通过文件的切片信息，获取文件名
            InputSplit insplit = context.getInputSplit();
            FileSplit fs=(FileSplit)insplit;
            filename = fs.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, Integer> word = new HashMap<String, Integer>();//先创建一个map集合用于统计所在行的相同单词出现个数

            //liangchaowei love liujialing
            String[] sp = value.toString().split(" ");
            //先判断访问的是文件
            if(filename.startsWith("mapreduce-4-1.txt")){
                //单词所在的文件中的行号
                count1++;
                for(String v:sp){
                    //判断是否添加过
                    if(word.containsKey(v)){
                        //所在行的单词个数
                        word.put(v,word.get(v)+1);
                    }else{
                        word.put(v, 1);
                    }
                }
                for(String k:word.keySet()){
                    mk.set(k);
                    //将文件名：行号，出现次数封装当value中
                    mv.set(filename+":"+count1+","+word.get(k));
                    System.out.println(word.get(k));
                    context.write(mk, mv);
                }
            }else{                                              //与上同理
                count2++;
                for(String v:sp){
                    if(word.containsKey(v)){
                        word.put(v,word.get(v)+1);
                    }else{
                        word.put(v, 1);
                    }
                }
                for(String k:word.keySet()){
                    mk.set(k);
                    mv.set(filename+":"+count2+","+word.get(k));
                    System.out.println(word.get(k));
                    context.write(mk, mv);
                }
            }
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text mv = new Text();
        //String[] GoodBean;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();               //创建一个包装类
            //txt.1:1   txt.1:1:2
            for(Text v:values){
                sb.append(v.toString()+";");       //将values内容进行拼接
            }
            mv.set(sb.toString());
            context.write(key, mv);
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(DXPX.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            Path inpath = new Path("hdfs://hadoop1:9000/dxpx_task");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/dxpx_task_out_2");
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
