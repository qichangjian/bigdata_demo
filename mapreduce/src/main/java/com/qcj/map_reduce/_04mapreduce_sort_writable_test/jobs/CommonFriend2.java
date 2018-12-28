package com.qcj.map_reduce._04mapreduce_sort_writable_test.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *   多任务调教  多job串联执行
 * 1、求所有两两用户之间的共同好友
 */
public class CommonFriend2 {
    static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text mk = new Text();
        Text mv = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //F-I	A (FI都有A好友)
            String[] datas = value.toString().split("\t");
            mk.set(datas[0]);
            mv.set(datas[1]);
            context.write(mk,mv);
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text mv = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text v:values) {
                sb.append(v).append("-");
            }
            //把key两个人的共同好友value都凭借在一起  A-F	C-E-O-D-B（AF的共同好友是ceodb）
            mv.set(sb.substring(0,sb.toString().length()-1));
            context.write(key,mv);
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(CommonFriend.class);
            job.setMapperClass(CommonFriend.MyMapper1.class);
            job.setReducerClass(CommonFriend.MyReducer1.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/commonfriend_in");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/commonfriend_in_one");
            FileOutputFormat.setOutputPath(job,outpath);

            //创建job2的对象
            Job job1 = Job.getInstance(configuration);
            job1.setJarByClass(CommonFriend2.class);
            job1.setMapperClass(MyMapper.class);
            job1.setReducerClass(MyReducer.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath1 = new Path("hdfs://hadoop1:9000/commonfriend_in_one");
            FileInputFormat.addInputPath(job1,inpath1);
            Path outpath1 = new Path("hdfs://hadoop1:9000/commonfriend_in_two");
            FileOutputFormat.setOutputPath(job1,outpath1);

            //如果是多job串联需要借助jobControl对象添加依赖关系
            //创建jobControl对象  参数：组名 会将添加到这个jc对象上的所有job放在一个组中 提交的时候相当于一个组中的job
            JobControl jc = new JobControl("bd1807");
            //添加job之间的依赖
            // ControlledJob 将原始的job转换为ControlledJob
            //第一个job转换  传入一个  参数：jobconf  job.getConfigration 可以得到
            ControlledJob cjob = new ControlledJob(job.getConfiguration());
            //第二个job
            ControlledJob cjob1 = new ControlledJob(job1.getConfiguration());

            //添加两个job的依赖关系
            cjob1.addDependingJob(cjob);//job1需要依赖job
            //添加到组中
            jc.addJob(cjob);
            jc.addJob(cjob1);

            //提交jc线程  implements Runable
            //创建线程对象
            Thread t = new Thread(jc);
            //启动线程
            t.start();

            //手动关闭  jc.allFinished()判断jc组中所有job是否执行完成
            while(!jc.allFinished()){
                Thread.sleep(500);//没有执行完就等待0.5s
            }
            jc.stop();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
