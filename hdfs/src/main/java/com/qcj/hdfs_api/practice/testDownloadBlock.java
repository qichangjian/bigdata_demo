package com.qcj.hdfs_api.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 手动拷贝某个特定的数据块（比如某个文件的第二个数据块）
 * 		某一个文件只下载第二个数据块
 * 		300M    blk1  0-127  blk2  128-255  blk3:256-300
 */
public class testDownloadBlock {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"),configuration,"hadoop1");
        //输入流
        FSDataInputStream in = fileSystem.open(new Path("/eclipse-jee-photon-R-win32-x86_64.zip"));
        //获取块信息
        //RemoteIterator迭代器  迭代  hasnext  next
        //LocatedFileStatus 文件状态对象  封装文件状态 文件路径  文件大小  文件副本
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/eclipse-jee-photon-R-win32-x86_64.zip"), false);
        //数据块数组
        //next对象获取的是每个对象    获取的是文件的状态信息  一个next代表一个文件 ，但是不一定是一个块
        BlockLocation[] blockLocations = locatedFileStatusRemoteIterator.next().getBlockLocations();
        //第二个数据块
        BlockLocation secondBlock = blockLocations[1];
        //取得第二个数据块的开始偏移量
        long offset = secondBlock.getOffset();
        //取得第二个数据块的长度
        long length = secondBlock.getLength();
        //从偏移量位置开始读取
        in.seek(offset);
        //输出到hdfs根目录
        //FSDataOutputStream out = fileSystem.create(new Path("/eclipse_second"));
        //创建本地输出流
        FileOutputStream out = new FileOutputStream("D:\\eclipse_second");
        //进行流的复制，并关闭流
        IOUtils.copyBytes(in,out,length,true);
    }
}
