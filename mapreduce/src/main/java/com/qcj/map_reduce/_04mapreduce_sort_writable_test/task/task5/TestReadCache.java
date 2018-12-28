package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task5;

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
import java.util.HashMap;
import java.util.Map;

public class TestReadCache {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(TestReadCache.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            /*//两个小文件地址
            URI uri1 = new URI("hdfs://hadoop1:9000/yingping_in/movies.dat");
            //URI uri2 = new URI(args[3]);
            job.addCacheFile(uri1);//不能漏掉！！！
            //job.addCacheFile(uri2);*/


            Path inpath = new Path("hdfs://hadoop1:9000/yingping_in");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/yingping_out");
            FileOutputFormat.setOutputPath(job,outpath);

            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } /*catch (URISyntaxException e) {
            e.printStackTrace();
        }*/
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private static Map<String, String> moivemap = new HashMap<String, String>();

       /* @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] paths = context.getLocalCacheFiles();
            //通过地址读取电影数据
            String strmoive = paths[0].toUri().toString();
            BufferedReader bf1 = new BufferedReader(new FileReader(new File(strmoive)));
            String stringLine = null;
            while((stringLine = bf1.readLine()) != null){
                System.out.println("DY:" + stringLine);

                String[] reads = stringLine.split("::");
                String moiveid = reads[0];
                String moiveInfo = reads[1] + "::" + reads[2];
                moivemap.put(moiveid, moiveInfo);
            }
            //关闭资源
            IOUtils.closeStream(bf1);
        }*/

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           /* String [] reads1 = value.toString().trim().split("::");
            String strmoive = moivemap.get(reads1[1]);
            System.out.println("strmoive:" + strmoive);
            context.write(new Text("1"),NullWritable.get());*/
           context.write(value,NullWritable.get());
        }
    }

    static class MyReducer extends Reducer<Text, NullWritable,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

}
