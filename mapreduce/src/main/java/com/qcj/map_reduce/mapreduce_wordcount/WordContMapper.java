package com.qcj.map_reduce.mapreduce_wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *  单词统计
 *
 *  extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *  mapper说明：
 *    输入和输出 类似于map类型
 *  KEYIN,     输入的键的类型：    这里指的是每一行的起始偏移量（Long类型的）
 *  VALUEIN,   输入的值的类型：    这里指的是一行的内容，这里的内容和上边的偏移量是一一对应的（String类型）
 *  IN代表输入
 *
 *   这里的输出的类型取决于本身业务
 *  KEYOUT,    输出的键的类型：  这里指的是每一个单词（String）
 *  VALUEOUT   输出的值的类型：  这里指的是单词的次数(int/long)
 *  OUT代表输出
 *
 * 注意：
 *    这里的数据类型不能使用java中的原生带的类型
 *    数据要经过网络传输，
 *    序列化：数据持久化存储 或者 网络传输（传输的一般都是01）的时候  数据需要序列化和反序列化
 *    张三 ---- 序列化 -----010101 -------反序列化 ------- 张三
 *    mapreduce编程中用于传输的数据类型必须是具有序列化和反序列化能力的
 *    hadoop中弃用了java中原生的serializable接口，实现了自己的一套序列化和反序列化接口Writable(只会对数据的值进行序列化)
 *    原因：java中的序列化和反序列化太重  繁琐
 *    long  1
 *
 *    对于一些常用的数据类型，hadoop已经实现
 *    int       IntWritable
 *    long      LongWritable
 *    string    Text(hadoop.io)
 *    byte      ByteWritable
 *    double    DoubleWritable
 *    float     FloatWritable
 *    boolean   BooleanWritable
 *    null      NullWritable
 *    自己定义的需要序列化和反序列化需要实现Writable接口
 */
public class WordContMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 重写map函数
     * @param key     输入的key:这里指的是每一行的起始偏移量，没有实际作用，只是一行的标示
     *                 hadoop底层数据读取的时候，字节读取的
     * @param value   输入的value:这里指的是一行的内容
     * @param context context上下文对象，用于远程传输的（map端-->Reduce端）
     * @throws IOException
     * @throws InterruptedException
     *
     * 当前函数的调用频率：
     *     一行调用一次（因为每次读取一行），如果一个文件有10行，当前函数会被调用10次
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //原来创建一个流，进行读取（mapreduce已经做了），每一行内容进行切分
        //1获取每一行内容  进行切分
        //将Text类型转换为String类型  text--tostring--string
        String line = value.toString();
        //2.切分
        String[] words = line.split("\t");
        //3.循环遍历每一个单词 进行统计，这里不统计，直接发送到reduce端统计（以key-value的形式发送）
        for (String w:words) {
            //将类型转化为序列化类型
            //string -- text
            Text mk = new Text(w);
            //int -- intwritable
            IntWritable mv = new IntWritable(1);
            //这里的write是直接写出，调用一次，写出一个到reduce端
            context.write(mk,mv);
        }
    }
}
