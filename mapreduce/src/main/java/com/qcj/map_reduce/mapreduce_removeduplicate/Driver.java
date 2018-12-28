package com.qcj.map_reduce.mapreduce_removeduplicate;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1、对以上的数据去重  分组
 */
public class Driver {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(Driver.class);
            job.setMapperClass(RemoveMapper.class);
            job.setReducerClass(RemoveReducer.class);

            //设置类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            //输入输出路径
            System.setProperty("HASOOP_USER_NAME","hadoop1");
            //Path inpath = new Path(args[0]);
            Path inpath = new Path("hdfs://hadoop1:9000/in_remove");
            FileInputFormat.addInputPath(job,inpath);
            //Path outpath = new Path(args[1]);
            Path outpath = new Path("hdfs://hadoop1:9000/out_remove_nojar");
            FileOutputFormat.setOutputPath(job,outpath);

            //提交
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
