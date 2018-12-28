package com.qcj.map_reduce.mapreduce_course_max_min_avg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 每一个course的最高分，最低分，平均分
 */
public class Driver {
    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(Driver.class);
            job.setMapperClass(CourseMapper.class);
            job.setReducerClass(CourseReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置切片大小
            FileInputFormat.setMaxInputSplitSize(job,1*1024);//要小于blocksize128  设置这个,eg:1K
            FileInputFormat.setMinInputSplitSize(job,200*1024*1024);//设置大于blocksize128,  设置这个，eg:200M

            /*Path inpath = new Path(args[0]);
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path(args[1]);
            FileOutputFormat.setOutputPath(job,outpath);*/
            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/out_remove_nojar");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_remove_nojar_avg");
            FileOutputFormat.setOutputPath(job,outpath);

            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
