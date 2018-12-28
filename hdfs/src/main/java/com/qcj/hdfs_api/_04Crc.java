package com.qcj.hdfs_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 通过文件下载验证crc校验文件：
 *      crc文件主要是：检查文件上传或者下载的过程中文件是否有丢失，主要是下载的时候
 */
public class _04Crc {
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(new URI("hdfs://hadoop01:9000"), conf,"hadoop");
        Path src=new Path("/testcrc");
        Path dst=new Path("D:\\test04");
        /**
         * 文件下载过程中 只要有一个是没有损坏的副本  下载是没有损坏的块  crc校验是可通过的
         *
         * crc校验的时候   校验的内容只是原始文件的偏移量内的内容  只要这部分内容没有发生变化  校验通过
         * 这部分内容  发生变化  校验不通过的
         *
         */
        fs.copyToLocalFile(src, dst);
        fs.close();
    }
}
