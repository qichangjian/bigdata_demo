package com.qcj.map_reduce._04mapreduce_sort_writable_test.customOutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


/**
 * 文件输出读取器
 */
public class MyRecordWriter extends RecordWriter<Text, DoubleWritable> {
    private FileSystem fs;
    Path path1 = new Path("hdfs://hadoop1:9000/score_out/jige/jige_stu");
    Path path2 = new Path("hdfs://hadoop1:9000/score_out/bujige/bujige_stu");

    FSDataOutputStream out1 = null;
    FSDataOutputStream out2 = null;
    public MyRecordWriter(FileSystem fs) throws IOException {
        //hdfs的输出流
        this.fs = fs;
        //流的初始化
        out1 = this.fs.create(path1);
        out2 = this.fs.create(path2);
    }
    //文件写出的方法

    /**
     * 获取hdfs的输入流
     */
    public void write(Text key, DoubleWritable value) throws IOException, InterruptedException {
        //获取分数
        double score = value.get();
        if(score >= 80){
            out1.write((key.toString() + "---" + score + "\n").getBytes());//写出
        }else{
            out2.write((key.toString() + "---" + score + "\n").getBytes());
        }
    }

    //关闭流的方法
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fs.close();
        out1.close();
        out2.close();
    }
}
