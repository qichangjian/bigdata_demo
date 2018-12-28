package com.qcj.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 自己写的通过流下载的方式
 *
 */
public class _06Define_IO {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		//文件上传   hdfs  读  输入流  ------------------   本地 写  输出流
		//创建hdfs的输入流 fs
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"),config,"hadoop1");

		FSDataInputStream in = fs.open(new Path("/NOTICE.txt"));

		//这个方法将流的指针设置到某一个字节开始读取   参数--偏移量
		in.seek(5L);//从第五个字节开始读取 和 下边的Long方法对应

		//创建本地输出流
		FileOutputStream out = new FileOutputStream("D:\\NOTICE.txt");

		//进行读写IOUtils hadoop进行文件读写的工具类
		//参数一：输入流  参数二：输出流 参数三：缓冲大小  参数四：代表执行完成是否关闭流
		//进行流的复制
		//IOUtils.copyBytes(in, out, 1024,true);
		//                       参数三 long类型是读取指定的字节数
		IOUtils.copyBytes(in, out, 10L,true);//in.seek(5L);//从第五个字节开始读取 和 下边的Long方法对应

	}
}
