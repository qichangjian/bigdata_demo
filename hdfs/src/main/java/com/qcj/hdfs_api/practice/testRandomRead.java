package com.qcj.hdfs_api.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

/**
 * 从随机地方开始读，读任意长度
 */
public class testRandomRead {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"),configuration,"hadoop1");
        FSDataInputStream in = fileSystem.open(new Path("/random.test"));
        Long beginNum = randomBeginLong();
        Long lengthNum = randomLengthLong();
        in.seek(beginNum);
        //创建本地输出流
        FileOutputStream out = new FileOutputStream("D:\\random.txt");
        //进行读写IOUtils hadoop进行文件读写的工具类
        IOUtils.copyBytes(in, out, lengthNum,true);//从第..个字节开始读取 和 下边的Long方法对应

    }

    //有范围的生成随机Long数
    public static Long randomBeginLong(){
        long min = 1L;
        long max = 10L;
        long rangeLong = min + (((long) (new Random().nextDouble() * (max - min))));
        System.out.println(rangeLong);
        return rangeLong;
    }

    //有范围的生成随机长度Long
    public static Long randomLengthLong(){
        long min = 1L;
        long max = 100L;
        long rangeLong = min + (((long) (new Random().nextDouble() * (max - min))));
        System.out.println(rangeLong);
        return rangeLong;
    }
}
