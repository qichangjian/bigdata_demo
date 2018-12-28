package com.qcj.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 自己写的通过流上传的方式
 */
public class _05Define_IO {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        //文件上传   本地  读  输入流  ------------------   hdfs 写  输出流
        //创建本地的输入流
        FileInputStream in = new FileInputStream("D:\\ip.txt");

        //创建hdfs的输出流 通过fs创建
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"),config,"hadoop1");

        FSDataOutputStream out = fs.create(new Path("/ip_io_idea.txt"));
        //进行读写IOUtils hadoop进行文件读写的工具类
        //参数一：输入流  参数二：输出流 参数三：缓冲大小  参数四：代表执行完成是否关闭流
        IOUtils.copyBytes(in, out, 1024,true);
        //                       参数三 long类型是读取指定的字节数
        //IOUtils.copyBytes(in, out, 8L,true);

    }
}

