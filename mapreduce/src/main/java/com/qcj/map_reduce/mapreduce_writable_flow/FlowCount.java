package com.qcj.map_reduce.mapreduce_writable_flow;

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
 * 流量统计
 */
public class FlowCount {

    //mapper类
    static class MyMapper extends Mapper<LongWritable, Text,Text,FlowBean>{
        Text mk = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");

            //获取需要的数据，进行封装发送
            mk.set(datas[1]);
            //封装fb
            int upflow = Integer.parseInt(datas[datas.length-3]);
            int downflow = Integer.parseInt(datas[datas.length-2]);
            FlowBean fb = new FlowBean(upflow,downflow);

            //发送
            context.write(mk,fb);
        }
    }
    /**
     * reducer类
     * mapreduce程序中 输出的key 和 values中分割的是\t
     * */
    static class MyReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            //相同的手机号为一组，存储在values中
            int sum_upFlow = 0;
            int sum_downFlow = 0;
            for (FlowBean fb:values) {
                sum_upFlow += fb.getUpFlow();
                sum_downFlow += fb.getDownFlow();
            }
            //封装为一个FlowBean，发送
            FlowBean res_fb = new FlowBean(sum_upFlow,sum_downFlow);

            //写出到hdfs磁盘  进行反序列化 调用toString方法
            context.write(key,res_fb);
        }
    }
    // main
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(FlowCount.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

            //设置切片大小
            /*
            FileInputFormat.setMaxInputSplitSize(job,1*1024);//要小于blocksize128  设置这个,eg:1K
            FileInputFormat.setMinInputSplitSize(job,200*1024*1024);//设置大于blocksize128,  设置这个，eg:200M
            */
            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/in_flow_bean");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/out_flow_bean");
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
