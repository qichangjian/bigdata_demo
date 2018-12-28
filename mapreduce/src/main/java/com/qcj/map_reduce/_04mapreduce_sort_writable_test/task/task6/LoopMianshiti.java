package com.qcj.map_reduce._04mapreduce_sort_writable_test.task.task6;

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
import java.util.ArrayList;
import java.util.List;

public class LoopMianshiti {
    static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text mk = new Text();
        Text mv = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\t");//八个空格
            //判断第二个字段是否为0
            if("0".equals(datas[1])){//父目录
                //1    0   家电
                mk.set(datas[0]);
                mv.set(datas[1]+"\t"+datas[2]);
                context.write(mk,mv);
            }else{//不是父目录
                //4    1   洗衣机
                mk.set(datas[1]);//发送自己的父目录，去关联父目录
                mv.set(datas[0] +"\t"+datas[2]);
                context.write(mk,mv);
            }
        }
    }

    static class MyReducer extends Reducer<Text,Text,Text,Text>{
        Text rk = new Text();
        Text rv = new Text();
        /**
         *  父目录
         *  key 1  value:0 家电
         *  子目录
         *  key 1  value:4 洗衣机
         *  key 1  value:5 冰箱
         */
        //相同的key是一组
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String partent = "";
            List<String> chileDir = new ArrayList<String>();
            //循环遍历
            for (Text v:values) {
                String tmp = v.toString();
                if(tmp.startsWith("0")){//父目录
                    partent = tmp;
                } else{//子目录
                    chileDir.add(tmp);
                }
            }
            //拼接的三种情况
            //没有父目录 partent = ""  例如：第一次只有1 2 3  继续输出等待自己的父目录拼接出来
            if("".equals(partent) && chileDir.size()!=0){
                // key：14  value 15：特仑苏
                //输出原始的数据   15 14 特仑苏
                String[] tmp01 = chileDir.get(0).split("\t");
                rk.set(tmp01[0]);
                rv.set(key.toString() + "\t" + tmp01[1]);
                context.write(rk,rv);
            }else if(chileDir.size()==0){//没有子目录
                //10 0 家电-洗衣机 -美的 没有子目录了
                //key 10  value 0 家电-洗衣机 -美的
                rv.set(partent);
                context.write(key,rv);
            }else{//都有
                /*
                一级目录
                key 1 value    0        家电
                   二级目录
                   vlaue
                   4       洗衣机
                   5       冰箱

                   循环遍历子目录和父目录拼接
                 */
                for(String c:chileDir){
                    //拼接成 4  0 家电-洗衣机
                    String[] temp02 = c.split("\t");
                    String[] temp03 = partent.split("\t");
                    rk.set(temp02[0]);
                    rv.set(temp03[0]+"\t"+temp03[1]+"-"+temp02[1]);
                    context.write(rk,rv);
                }
            }
        }
    }

    public static void main(String[] args) {
        int count = 0;
        while(true){
            count ++;
            Configuration configuration = new Configuration();
            System.setProperty("HASOOP_USER_NAME","hadoop1");
            try {
                Job job = Job.getInstance(configuration);
                job.setJarByClass(LoopMianshiti.class);
                job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                if(count ==1){//可以用全局计数器设计
                    Path inpath = new Path("hdfs://hadoop1:9000/mianshiti1_in");
                    FileInputFormat.addInputPath(job,inpath);
                }else{
                    Path inpath = new Path("hdfs://hadoop1:9000/mianshiti1_out"+(count-1));
                    FileInputFormat.addInputPath(job,inpath);
                }
                Path outpath = new Path("hdfs://hadoop1:9000/mianshiti1_out"+count);
                FileOutputFormat.setOutputPath(job,outpath);

                job.waitForCompletion(true);
                if(count == 3){
                    break;
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
