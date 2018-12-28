package com.qcj.map_reduce._04mapreduce_sort_writable_test.customInput;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义输入
 * 原始的默认输入是按行输入的，如果不按行输入（Sqoop），怎么办？
 *
 * 需求：进行小文件合并
 *      文件读取：一次读取一个文件内容就行
 *    需要自定义输入：
 *        默认输入FileInputFormat实现类TextInputFormat
 *        createRecordReader
 *          RecordReader---LineRecordReader
 *             nextKeyValue getCurrentKey getCurrentValue
 *
 *   自定义输入：
 *       1.自定义一个FIleInputFormat的子类TextInputFormat
 *         重写createRecordReader
 *       2.自定义一个文件读取器：LineRecordReader继承LineRecordReader抽象类
 *         重写三个方法nextKeyValue getCurrentKey getCurrentValue
 */
/*
自定义文件加载：
    泛型：指的是最终文件读取完成的key和value的泛型  读取完成--Mapper
    一次读取一个文件
        文件内容
 */
public class MyFileInputFormat extends FileInputFormat<NullWritable, Text> {

    public RecordReader<NullWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        MyRecordReader mrr = new MyRecordReader();
        mrr.initialize(inputSplit,taskAttemptContext);//参数一：切片参数二：上下文对象
        return mrr;
    }
}
