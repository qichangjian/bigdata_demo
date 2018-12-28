package com.qcj.map_reduce.mapreduce_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 单词统计
 * 输入：reduce的数据来源于map
 * KEYIN,   输入的类型   这里指的是map输出的类型  Text
 * VALUEIN, 输入的value的类型   这里指的是map输入出的value的类型  intwritable
 *
 * 输出：
 * KEYOUT,  输出的key   这里指的是单词的类型 Text
 * VALUEOUT 输出的value  这里指的是单词的总次数 intwriable
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 重写reduce方法:分组做统计
     *   到reduce端的数据 是已经分好组的数据
     *   默认情况下：按照map输入的key进行分组，将map输出的key相同的分为一组
     * @param key          这里的key指定是每一组中的相同的key（单词）
     * @param values       这里指的是每一组中的所有的value值，封装到一个迭代器中了（1,1,1，...）
     * @param context      上下文对象：用于传输的   数据写出到hdfs中
     * @throws IOException
     * @throws InterruptedException
     *
     * 调用频率：
     *    一组调用一次，每一次统计一个单词的最终结果
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //循环遍历value进行求和
        int sum = 0;
        for (IntWritable v:values) {
            //进行类型转换
            //intwritable -- int  数值类型的 get 方法  将hadoop中的数据类型转换为java中的类型
            sum += v.get();
        }
        //写出结果文件
        //转换类型
        IntWritable rv = new IntWritable(sum);
        context.write(key,rv);
    }
}
