package com.qcj.map_reduce._04mapreduce_sort_writable_test.mapJoin.yingping_anli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * 影评案例:三表连接
 *    两个小表放在缓存中： 用到缓存 需要打jar包到集群上运行，不然会报错
 */
public class YingPing {
    /**
     * key输出：关联键
     * value输出：输出关联结果
     */
    static class MyMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private static Map<String, String> moivemap = new HashMap<String, String>();
        private static Map<String, String> usersmap = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //读取t_product  读取后存放在 map集合（key：关联字段）容器中：
            //获取文件的缓存路径
            Path[] localCacheFiles = context.getLocalCacheFiles();

                     //读取电影数据
            //创建一个流进行读取
            BufferedReader br = new BufferedReader(new FileReader(localCacheFiles[0].toString()));
            //读取
            String line = null;
            while ((line=br.readLine())!=null){
                String[] datas = line.split("::");
                if(datas.length==3) {
                    String moiveid = datas[0];//
                    //性别，年龄
                    String moiveInfo = datas[1] + "::" + datas[2];
                    moivemap.put(moiveid, moiveInfo);
                }
            }
                   //读取用户数据
            //创建一个流进行读取
            BufferedReader br2 = new BufferedReader(new FileReader(localCacheFiles[1].toString()));
            //读取
            String line2 = null;
            while ((line2=br2.readLine())!=null){
                String[] datas = line2.split("::");
                if(datas.length==5){
                    String userid = datas[0];//
                    //性别，年龄
                    String userInfo = datas[1] + "::" + datas[2]+ "::" + datas[3]+ "::" + datas[4];
                    usersmap.put(userid,userInfo);
                }

            }
            //关闭资源
            IOUtils.closeStream(br);
            IOUtils.closeStream(br2);
        }

        Text mk = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取大文件中的信息
            String[] datas = value.toString().split("::");
            //根据 userid 和 moveId 获取文件中数据 value数据
            String struser = usersmap.get(datas[0]);
            String strmovie = moivemap.get(datas[1]);

            //进行三表拼接
            //各自数据放在数组中
            String [] userinfo = struser.split("::");//gender, age, occupation, zipcode
            String [] moiveinfo = strmovie.split("::");//movieName, movieType
            String kk = datas[0] + "::" + datas[1] + "::" + datas[2] + "::" + datas[3] + "::"
                    + userinfo[0] + "::" + userinfo[1] + "::" + userinfo[2] + "::" + userinfo[3] + "::"
                    + moiveinfo[0] + "::" + moiveinfo[1];

            mk.set(kk);

            context.write(mk,NullWritable.get());
        }
    }


    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        System.setProperty("HASOOP_USER_NAME","hadoop1");
        configuration.set("fs.defaultFS","hdfs://hadoop1:9000");
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(YingPing.class);
            job.setMapperClass(MyMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            //两个小文件地址
            URI uri2 = new URI("hdfs://hadoop1:9000/YingPing_in/users.dat");
            URI uri1 = new URI("hdfs://hadoop1:9000/YingPing_in/movies.dat");
            job.addCacheFile(uri1);//不能漏掉！！！
            job.addCacheFile(uri2);

            System.setProperty("HASOOP_USER_NAME","hadoop1");
            Path inpath = new Path("hdfs://hadoop1:9000/YingPing_in/ratings.dat");
            FileInputFormat.addInputPath(job,inpath);
            Path outpath = new Path("hdfs://hadoop1:9000/YingPing_out");
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
