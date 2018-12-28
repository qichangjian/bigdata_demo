package com.qcj.hdfs_api.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 删除HDFS集群中的所有空文件和空目录
 *
 *  * 删除HDFS集群中的所有空文件和空目录   /
 *  *      判断给定的目录是否是空目录;
 *         获取给定的目录下的所有的文件或目录:
 *             如果是文件：
 *                 判断文件的长度是否为0
 *                     0   删除
 *             如果是目录：
 *                 判断是否是空目录：
 *                     是：删除
 *                     不是：递归 操作
 *                 父目录是否是空目录
 *
 */
public class testDeleteEmptyFile {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        //fs
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"),configuration,"hadoop1");
        Path path = new Path("/");
        deleteEmptyFile(fileSystem,path);
    }

    public static void deleteEmptyFile(FileSystem fs,Path path) throws IOException {
        //获取路径下的文件和文件夹列表
        FileStatus[] listStatus = fs.listStatus(path);
        //判断是否是空目录
        if(listStatus.length == 0){
            fs.delete(path,false);
        }else{
            //不是空目录遍历
            for (FileStatus fileStatus:listStatus) {
                Path cpath = fileStatus.getPath();//目录下单个
                if(fileStatus.isFile()){//是否是文件
                    if(fileStatus.getLen() == 0){
                        fs.delete(cpath,false);
                    }
                }else{//不是文件
                    deleteEmptyFile(fs,cpath);//递归
                }
            }
            //经过上边删除后目录是否为空
            FileStatus[] listStatus2 = fs.listStatus(path);
            if(listStatus2.length == 0){
                deleteEmptyFile(fs,path);
            }
        }
    }


}
