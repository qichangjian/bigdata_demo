package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task3;

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
 * 2.1统计每门课程参考学生的平均分，并且按课程存入不同的结果文件
 *   就是按照课程分区partition
 */
public class PartByScore_StudentAvg {
    static class MyMapper extends Mapper<LongWritable, Text,ScoreBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split(",");
            int count = 0;
            int sum = 0;
            for (int i = 2; i < datas.length; i++) {
                count ++;
                sum += Integer.parseInt(datas[i].trim());
            }
            ScoreBean scoreBean = new ScoreBean(datas[0],datas[1],(sum / count));
            context.write(scoreBean,NullWritable.get());
        }
    }

    static class MyReducer extends Reducer<ScoreBean, NullWritable,ScoreBean, NullWritable>{
        @Override
        protected void reduce(ScoreBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (NullWritable n:values) {
                count ++;
                if(count > 1){
                    break;
                }
                context.write(key,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(PartByScore_StudentAvg.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(ScoreBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(ScoreBean.class);
            job.setOutputValueClass(NullWritable.class);

            //指定自定义分区
            job.setPartitionerClass(MyPartitioner.class);
            //指定reducetask个数
            //启动三个reduce:一个reducetask会对应一个结果文件
            job.setNumReduceTasks(4);//这是reducetask的并行度为3

            //设定分组
            job.setGroupingComparatorClass(MyGroup.class);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/in_avg_score");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/partByScore_studentAvg_one");
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
