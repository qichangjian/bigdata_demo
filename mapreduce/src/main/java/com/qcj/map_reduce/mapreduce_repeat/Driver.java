package com.qcj.map_reduce.mapreduce_repeat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 3、求该成绩表当中出现相同分数的分数，还有次数，以及该分数的人数
 求相同科目相同分数的有多少个人  并且都是谁
 返回结果的格式：
 科目	分数	次数	该分数的人
 例子：
 computer	85	3	huangzitao,liujialing,huangxiaoming

 mapreduce  ---分组----map的key-----reduce
 */
public class Driver {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(Driver.class);
            job.setMapperClass(RepeatMapper.class);
            job.setReducerClass(RepeatReduce.class);

            //设置类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            //输入输出路径
            Path inpath = new Path(args[0]);
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path(args[1]);
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
