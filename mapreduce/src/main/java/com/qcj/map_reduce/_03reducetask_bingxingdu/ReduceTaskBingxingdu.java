package com.qcj.map_reduce._03reducetask_bingxingdu;

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
 * 3.统计每一个学生的每一门课程的平均分
 *
 * reduce并行度   设置为3
 */
public class ReduceTaskBingxingdu {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ReduceTaskBingxingdu.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //启动三个reduce:一个reducetask会对应一个结果文件
            job.setNumReduceTasks(3);//这是reducetask的并行度为3

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/in_avg_score");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_avg_score_reduceTask");
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

    static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text mk = new Text();
        Text mv = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            mk.set(datas[1]);//学生
            StringBuilder score_score = new StringBuilder();
            score_score.append(datas[0]).append(",");
            for (int i = 2; i < datas.length; i++) {
                if(i==datas.length-1){
                    score_score.append(datas[i]);
                }else{
                    score_score.append(datas[i]).append(",");
                }
            }
            mv.set(score_score.toString());
            context.write(mk,mv);
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text rv = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int scoreAll=0;
            int scoreNum=0;
            for (Text v:values) {
                String[] oneScore = v.toString().split(",");
                StringBuilder sbrv = new StringBuilder();
                sbrv.append(oneScore[0]).append("\t");//添加课程
                for (int i = 1; i < oneScore.length; i++) {
                    int score = Integer.parseInt(oneScore[i]);
                    scoreAll += score;
                    scoreNum ++;
                }
                rv.set(sbrv.append(scoreAll/scoreNum).toString());
                context.write(key,rv);
            }
        }
    }
}
