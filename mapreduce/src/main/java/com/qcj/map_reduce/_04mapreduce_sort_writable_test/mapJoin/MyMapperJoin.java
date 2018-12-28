package com.qcj.map_reduce._04mapreduce_sort_writable_test.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * map端的join
 * java.lang.Exception: java.io.FileNotFoundException: file:\tmp\hadoop-Administrator\mapred\local\1539656968499\t_product (文件名、目录名或卷标语法不正确。)
 * 这种方式只能打jar包运行
 *
 */
public class MyMapperJoin {
    /**
     * key输出：关联键
     * value输出：输出关联结果
     */
    static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
        Map<String,String> productMap = new HashMap<String, String>();
        Text mv = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //读取t_product  读取后存放在 map集合（key：关联字段）容器中：
            //获取文件的缓存路径
            Path[] localCacheFiles = context.getLocalCacheFiles();
            //创建一个流进行读取
            BufferedReader br = new BufferedReader(new FileReader(localCacheFiles[0].toString()));
            //读取
            String line = null;
            while ((line=br.readLine())!=null){
                String[] datas = line.split("\t");
                productMap.put(datas[0],datas[1]+"\t"+datas[2]+"\t"+datas[3]);//这个是多对一中的一，如果是多需要连接
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取文件中的信息
            String[] datas = value.toString().split("\t");
            //匹配 进行关联
            if(productMap.containsKey(datas[2])){
                //进行拼接
                String res = productMap.get(datas[2]) + "\t"+datas[0]+ "\t"+datas[1]+ "\t"+datas[3];
                mv.set(res);
                context.write(new Text(datas[2]),mv);
            }
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        configuration.set("fs.defaultFS","hdfs://hadoop1:9000");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(MyMapperJoin.class);
            job.setMapperClass(MyMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //将reduceTask的个数设置为0
            job.setNumReduceTasks(0);

            //将指定的路径文件加载到每个maptask的缓存中
            job.addCacheFile(new URI("/join_in/t_product"));


            Path inpath = new Path("/join_in/t_order");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("/join_out_mapjoin");
            FileOutputFormat.setOutputPath(job,outpath);

            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
