package com.qcj.map_reduce._00task.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *  第一题、HDFS题综合练习
 *     编写程序统计出HDFS文件系统中文件大小小于HDFS集群中的默认块大小的文件占比
 *     比如：大于等于128M的文件个数为98，小于128M的文件总数为2，所以答案是2%
 */
public class Task1 {
    static int DEFAULT_BLOCK = 128*1024*1024;//默认块大小128M

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        //fs
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"),configuration,"hadoop1");
        //路径
        Path path = new Path("/");
        //设置指定目录下文件个数以及小于128M的文件个数
        int minNumber = 0;
        int allNumber = 0;
        //LocatedFileStatus 文件状态对象  封装文件状态 文件路径  文件大小  文件副本
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(path,false);
        while(locatedFileStatusRemoteIterator.hasNext()){
            allNumber ++;
            //获取文件大小
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            long len = next.getLen();
            if(len < DEFAULT_BLOCK){
                minNumber ++;
            }
        }
        //计算占比
        String result = ((minNumber*1f)/allNumber * 100)+"%";
        System.out.println("小于个数："+minNumber +" | 总文件个数：" + allNumber);
        System.out.println( "文件占比：" + result);
        //关闭流
        fileSystem.close();
    }
}
