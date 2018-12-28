package com.qcj.map_reduce._04mapreduce_sort_writable_test.customOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义输出
 *    不同结果输出到不同的文件夹下的不同文件下
 *    仅仅决定的是reduce处理完成的数据的输出目录 或者格式的问题，并不决定reduce的数据如何分配（分区决定）
 *
 *    自定义输入：
 *  *  FileOutputFormat-TextOUtputPormat
 *  *  RecordWriter -- lineRecordWriter
 *  *  对应的是reducetask的输出
 *  *     默认只能输出到同一个文件夹下的同一个文件
 *  *
 *  *  需求：输出到不同文件夹下的不同文件
 *  *    成绩 > 60   -->  /score_out/jige/jige_stu
 *  *    <60         -->  /score_out/bujige/bujige_stu
 */
public class Score_DiffDic {
    static class MyMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        Text mk = new Text();
        DoubleWritable mv = new DoubleWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");
            if(datas.length==3){
                mk.set(datas[0]);
                mv.set(Double.parseDouble(datas[2]));
                context.write(mk,mv);
            }
        }
    }

    static class MyReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable v:values) {
                context.write(key,v);
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        configuration.set("fs.defaultFs","hdfs://hadoop1:9000");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(Score_DiffDic.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            //设置job的输入为自定义输入
            job.setOutputFormatClass(MyFileOutputFormat.class);

            Path inpath = new Path("hdfs://hadoop1:9000/out_avg_score_sort");
            FileInputFormat.addInputPath(job,inpath);
            //用自定义的输出
            //这里的是存放运行结果输出成功文件的
            Path outpath = new Path("hdfs://hadoop1:9000/diff_out");
            MyFileOutputFormat.setOutputPath(job,outpath);

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
