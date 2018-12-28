package com.qcj.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import java.net.URISyntaxException;

/**
 * hdfs api操作：
 *      文件上传(这个是本地文件上传到本地（项目路径下）)
 *      不需要core-site.xml，如果有就会报错
 *
 * 执行步骤：
 *     1.导入maven依赖
 *     2.编写代码
 */
public class _01HDFS_API {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        //加载hdfs的配置文件
        /**
         * Configuration  配置文件对象    加载配置文件  或设置配置文件
         * 这里的配置文件加载的是集群的？  还是本地的？ 集群的
         */
        Configuration conf = new Configuration();
        //获取hdfs的操作对象   获取的是hdfs的操作句柄对象  FileSystem对象
        /**
         * hdfs的文件系统对象   想要操作hdfs必须先拿到这个对象
         * hadoop fs
         * 底层IO操作
         * 这个fs对象是谁的文件系统对象？  集群的？本地的？
         */
        FileSystem fs = FileSystem.get(conf);
        //org.apache.hadoop.fs.LocalFileSystem@5bd03f44  本地文件系统对象
        //文件上传  和下载  都在本地操作的
        System.out.println(fs);

        //参数1---本地文件 windows   参数2---目标文件路径   --hdfs
        //Path   hdfs的文件对象  路径对象   java--File
        Path src = new Path("D:\\1.txt");
        Path dst = new Path("/");//上传到了本地也就是windows项目的根目录下也就是c盘下了
        //文件上传
        /**
         * put 本地  hdfs路径
         * copyfromlocal-----api
         * movefromlocal
         */
        //文件上传  上传到本地     文件传到了工程的根目录下（c盘下）
        //文件上传的时候生成了两个文件   原始文件movie.dat   另外一个文件  .movies.dat.crc  作用是什么？
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }
}
