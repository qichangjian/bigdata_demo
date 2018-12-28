package com.qcj.hdfs_api.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 级联删除HDFS上的某个文件夹（级联删除）  自己写递归
 */
public class testDeleteFile {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration config = new Configuration();
        //fs
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"),config,"hadoop1");
        Path path = new Path("/");
        deleteFile(fs,path);//调用递归删除
    }

    //删除
    public static void deleteFile(FileSystem fs,Path deletepath) throws IOException {
        FileStatus[] listStatus = fs.listStatus(deletepath);
        //判断是否是一个空目录
        /*if(listStatus.length==0){
            fs.delete(deletepath,false);
        }else {
            for (FileStatus fileStatus : listStatus) {
                //判断当前迭代对象是否是目录
                boolean isDir = fileStatus.isDirectory();
                if(isDir){
                    Path nowpath = fileStatus.getPath();
                    deleteFile(fs,nowpath);
                }else{
                    Path nowpath = fileStatus.getPath();
                    fs.delete(nowpath,false);//不级联删除
                    deleteFile(fs,deletepath);//删除后判断是否为空了
                }

            }
        }*/
        if(listStatus.length==0){
            System.out.println("是一个空目录。。。删除"+deletepath);
            fs.delete(deletepath,false);
        }else{
            System.out.println("不是一个空目录。。。");
            for (FileStatus fileStatus : listStatus) {
                //判断当前迭代对象是否是目录
                boolean isDir = fileStatus.isDirectory();
                if (isDir) {
                    Path nowpath = fileStatus.getPath();
                    System.out.println("else：是一个目录"+nowpath);
                    deleteFile(fs, nowpath);
                } else {
                    Path nowpath = fileStatus.getPath();
                    System.out.println("else：是一个文件，删除");
                    fs.delete(nowpath,false);//不级联删除
                    //
                }
            }
            deleteFile(fs, deletepath);//删除后判断是否为空了
        }
    }
}
