package com.qcj.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
/**
 *文件上传（本地到HDFS路径）
 * 执行步骤：
 *     1.开启集群：
 *          zkServer.sh start
 *          start-dfs.sh
 *          hdfs上创建一个目录用于测试
 *          [hadoop1@hadoop3 ~]$ hdfs dfs -mkdir /hdfsapi
 *     2.导入maven依赖
 *     3.将core-site.xml拷贝到resources目录下
 *     4.编写代码
 */
public class _02HDFS_API {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        //key属性名  value：属性值
        //System.setProperty("HADOOP_USER_NAME", "hadoop");
        //1.加载配置文件  2.设置配置文件的参数
        /**
         * 配置文件加载的是默认的配置文件
         * 默认加载的配置文件是  工程下hadoop-hdfs-2.7.6.jar的jar包下的hdfs-default.xml这个文件
         * 进行文件上传的时候 所有的参数都是默认的参数
         * Configuration对象默认加载：
         * 		hdfs-default.xml
         * 		mapred-default.xml
         * 		yarn-default.xml
         * 		core-default.xml
         * 想要加载自己的配置文件，需要将自己的配置文件拷贝到工程的src下  才能，默认加载到自己的配置文件
         * 		自动加载的配置文件只能识别
         * 			hdfs-default.xml
         * 			hdfs-site.xml
         * 			mapred-default.xml
         * 			mapred-site.xml
         * 			yarn-default.xml
         * 			core-default.xml
         *  * 		yarn-site.xml
         * 			core-site.xml
         * 配置如果没有放在src下？需要代码中手动加载配置文件
         * 		conf.addResource("conf/hdfs-site.xml");
         * 		在代码中也可以手动设置配置文件
         * 配置文件的加载顺序：
         * 	1）jar包
         * 2）src下
         * 3）代码中的
         * 生效：
         * 	3》》》2》》》》1
         *
         */
        Configuration conf=new Configuration();
        //conf.addResource("conf/hdfs-site.xml");
        conf.set("dfs.replication", "4");
        //没有指定集群的连接入口  想要获取集群的需要指定集群的连接入口
        //在conf对象上设置连接入口
        /**
         * 参数1：配置文件的属性名
         * 参数2：配置文件的属性的值
         */
        //conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        //参数1：namenode的连接入口 URI conf user
        FileSystem fs=FileSystem.get(new URI("hdfs://bd1807/hadoop1:9000"), conf, "hadoop1");
        //DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-1816936482_1, ugi=Administrator (auth:SIMPLE)]]
        //这次获取的对象是分布式文件系统的对象
        //返回的对象的类型DistributedFileSystem
        System.out.println(fs instanceof DistributedFileSystem);
        //文件上传
        Path src=new Path("D:\\1.txt");//win的路径
        Path dst=new Path("/hdfsapi/1.txt");//hdfs的路径
        /**
         * 文件上传的时候  报一个权限的问题  原因是用户在eclipse中进行上传文件的时候  使用windows下的用户  而不是hdfs的安装用户
         * 解决权限问题：
         * 	1）在代码提交的时候指定用户
         * 	代码运行的时候  右键》》 run configurations>> 配置运行代码需要的参数
         * 		program arguments:代码程序运行需要的参数
         * 			代码中需要控制台传入的参数  这个参数通过main(String[] args)
         *		VM arguments:JVM运行过程中需要的参数
         *			比如jvm内存大小   jvm运行的用户指定
         -DHADOOP_USER_NAME=hadoop
         2）在代码中指定用户
         1)指定系统的运行参数  System
         System.setProperty("HADOOP_USER_NAME", "hadoop");
         2)FileSystem fs=FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");
         3)windows下添加一个hadoop用户  不建议
         *
         */
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }
}
