package com.qcj.map_reduce.mapreduce_max_min_avg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 求最大值，最小值，平均值
 * 文本中是数字，用\t分割的
 */
public class Driver {
    public static void main(String[] args) {
        //将mapper reducer类进行封装，封装为一个任务--job(作业)
        //加载配置文件
        Configuration configuration = new Configuration();
        //启动一个job,  就是创建一个job对象
        try {
            Job job = Job.getInstance(configuration);
            //设置这个job,将map和reduce绑定一下
            //设置整个job的主函数入口
            job.setJarByClass(Driver.class);
            //设置整个job的mapper对应的类
            job.setMapperClass(MyMapper.class);
            //设置真个job的reducer对象的类
            job.setReducerClass(MyReduce.class);

            //设置map输出key的类型  value的类型
            //指定了泛型，这里为什么还要设置一次    泛型的作用范围：编译的时候生效，运行的时候泛型会自动擦除
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置reduce输出的key的类型 value的类型  以下方法设置的是mr的最终输出
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            //指定需要统计的文件的输入路径，靠文件输入类FileInputFormat（抽象类）
            //输入路径
            Path path = new Path(args[0]);
            //Path path = new Path(args[0]);//控制台传参
            FileInputFormat.addInputPath(job,path);

            //指定输出目录(输出路径一定不能存在，否则会报错  默认输出是覆盖式的输出，如果给的输出目录存在，有可能造成原始数据丢失)
            Path outpath = new Path(args[1]);
            FileOutputFormat.setOutputPath(job,outpath);

            //提交整个job
            //job.submit();//提交当前job,这个方法不打印日志，一般不常用
            //执行这一句的时候真个job才会提交，上边做的一些列的工作都是为了设置job的，而不是为了提交job的
            job.waitForCompletion(true);//参数是否添加打印日志

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
