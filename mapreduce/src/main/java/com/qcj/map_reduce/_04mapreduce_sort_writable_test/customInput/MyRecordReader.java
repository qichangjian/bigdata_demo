package com.qcj.map_reduce._04mapreduce_sort_writable_test.customInput;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 文件读取器
 */
public class MyRecordReader extends RecordReader<NullWritable, Text> {
    //读取的map的value也就是真个文件的内容
    Text map_value = new Text();

    FileSystem fs = null;//fs对象
    FSDataInputStream in = null;//流
    //定义一个读取变量：判断当前文件是否读取完成
    boolean isReader = false;//判断是否读取完成
    long length;

    /**
     * 在整个类的最开始运行
     * @param inputSplit   输入文件切片信息，一个切片对应一个maptask任务，调用一次这个方法
     * @param taskAttemptContext 任务执行的上下文对象
     */
    //初始化方法  类似于setup方法，做一些属性 或者流的初始化
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //获取fs对象  local 和 distribute两种fs对象
        fs = FileSystem.get(taskAttemptContext.getConfiguration());//上下文对象中获得配置文件
        //创建流
        FileSplit fss= (FileSplit)inputSplit;
        in = fs.open(fss.getPath());//path就是切片路径
        length = fss.getLength();//切片的长度
    }

    /**
     * 进行一个文件的读取，当前文件，读取到末尾就不需要读取了
     *  将最终读取的结果放在value上
     *
     *  需要流 ：hdfs的输入流
     *      如何创建：fs.open(path)
     *          fs对象怎样创建？initialize中
     *
     */
    //判断是否还有下一个
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //进行文件读取
        if(!isReader){//没有读取完成
            //开始进行读取
            /**
             * readFully：
             * 参数一：读取的字节数组
             * 参数二：开始读取的偏移量
             * 参数三：读取的长度，整个切片的长度
             */
            //这里整个文件-是一个文件切片，只读取一次
            byte[] b = new byte[(int)length];
            in.readFully(b,0,(int)length);
            //读取的东西赋值给value
            map_value.set(b);
            isReader = true;
            return isReader;//mapTask中while（isreader）
        }
        return false;
    }
    //获取给Map的key
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();//单例设计模式
    }
    //获取给Map的value
    public Text getCurrentValue() throws IOException, InterruptedException {

        return map_value;
    }
    //获取进度的方法  当前文件读取了多少
    public float getProgress() throws IOException, InterruptedException {
        return isReader?1.0f:0.0f;
    }
    //关闭的方法，进行流的关闭或者变量的销毁工作
    public void close() throws IOException {
        in.close();
        fs.close();
    }
}
