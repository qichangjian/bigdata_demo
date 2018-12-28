package com.qcj.map_reduce._04mapreduce_sort_writable_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 3、统计每一个学生的每一门课程的平均分
 *
 * 只要mapper类的做法，学生和课程都放在key中 平均分放在map
 *
 */
public class StudentCountAndAVG2 {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(StudentCountAndAVG2.class);
            job.setMapperClass(MyMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            //默认情况下，会自动调用一次reduce
            //如果不需要reduce 一定要加这句话
            //***如果直接用map输出，结果是对的但是没有排序
            job.setNumReduceTasks(0);//参数设置为0.意味着不需要reduce

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/in_avg_score");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_avg_score11");
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

    /**
     * 学生和课程都在key中
     * value中是平均封
     */
    static class MyMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        Text mk = new Text();
        DoubleWritable mv = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            mk.set(datas[0]+"\t"+datas[1]);
            //求平均分
            int sum = 0;
            int count = 0;

            for (int i = 2; i < datas.length; i++) {
                count ++;
                sum += Integer.parseInt(datas[i]);
            }
            double avg = 1.0*sum/count;
            mv.set(avg);
            context.write(mk,mv);
        }
    }
}
