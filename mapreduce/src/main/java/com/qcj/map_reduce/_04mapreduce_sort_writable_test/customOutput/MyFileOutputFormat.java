package com.qcj.map_reduce._04mapreduce_sort_writable_test.customOutput;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输入：
 *  FileOutputFormat-TextOUtputPormat
 *  RecordWriter -- lineRecordWriter
 *  对应的是reducetask的输出
 *     默认只能输出到同一个文件夹下的同一个文件
 *
 *  需求：输出到不同文件夹下的不同文件
 *    成绩 > 60   -->  /score_out/jige/jige_stu
 *    <60         -->  /score_out/bujige/bujige_stu
 */
public class MyFileOutputFormat extends FileOutputFormat<Text, DoubleWritable> {
    /**
     *
     * @param taskAttemptContext 上下文对象
     */
    public RecordWriter<Text, DoubleWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //获取filesystem对象
        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
        return new MyRecordWriter(fs);
    }
}
