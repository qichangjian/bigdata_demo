package com.qcj.hdfs_api.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 删除某个路径下特定类型的文件，比如class类型文件，比如txt类型文件
 */
public class testDeleteFileBySuffix {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"),configuration,"hadoop1");
        Path path = new Path("/");
        String endString = ".ccc";
        deleteFileBySuffix(fileSystem,path,endString);
    }

    public static void deleteFileBySuffix(FileSystem fs,Path path,String endString) throws IOException {
        //获取文件下文件夹和目录列表
        FileStatus[] listStatus = fs.listStatus(path);
        //列表不为空
        if(listStatus.length != 0){
            for (FileStatus fileStatus:listStatus) {
                Path newPath = fileStatus.getPath();
                //是否是文件
                if(fileStatus.isFile()){
                    //判断是否以..结尾 是就删除
                    if(newPath.toString().endsWith(endString)){
                        fs.delete(newPath,false);
                    }
                }else{
                    deleteFileBySuffix(fs,newPath,endString);
                }
            }
        }

    }
}
