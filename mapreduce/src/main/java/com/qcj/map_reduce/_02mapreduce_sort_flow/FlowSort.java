package com.qcj.map_reduce._02mapreduce_sort_flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 流量排序： 总流量倒叙  手机号升序
 *
 * 1.对于自定义类的排序想记住shuffle过程排序，自定义的类必须放在map的key中
 * 2.自定义类必须实现WritableComparable接口
 * 3.当自定义的类放在map的key位置上，就会默认按照他进行排序，所以自定义类必须实现WritableComparable接口
 */
public class FlowSort {
    static class MyMapper extends Mapper<LongWritable, Text,FlowBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取一行数据样式： 手机号 上行流量 下行流量 总流量
            String[] datas = value.toString().split("\t");
            //封装flowBean进行发送
            FlowBean fb = new FlowBean(datas[0],Integer.parseInt(datas[1]),Integer.parseInt(datas[2]),Integer.parseInt(datas[3]));
            context.write(fb,NullWritable.get());
        }
    }

    /**
     * 总流量：按照comparaTo分组，也就是排序结果相同的 :也就是手机号和总流量都相同在一组
     */
    static class MyReducer extends Reducer<FlowBean,NullWritable,FlowBean,NullWritable> {
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            //输出结果
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(FlowSort.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(FlowBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(FlowBean.class);
            job.setOutputValueClass(NullWritable.class);

            //设置切片大小
            /*
            FileInputFormat.setMaxInputSplitSize(job,1*1024);//要小于blocksize128  设置这个,eg:1K
            FileInputFormat.setMinInputSplitSize(job,200*1024*1024);//设置大于blocksize128,  设置这个，eg:200M
            */
            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/out_flow_bean");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_flow_bean_sort");
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
