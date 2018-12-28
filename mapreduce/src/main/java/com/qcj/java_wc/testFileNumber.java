package com.qcj.java_wc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 *  6个文件   每个文件中存放的都是数字  每个数字之间的分割符，
 * 		求6个文件中所有的数字的最大值  最小值  平均值
 */
public class testFileNumber {
    static float avg=0,max=0,min=1000,count=0;
    static int number=0;
    public static void main(String[] args) throws IOException {
        oneFile("D:\\11.txt","D:\\12.txt","D:\\13.txt","D:\\14.txt","D:\\15.txt","D:\\16.txt");
    }

    public static void oneFile(String ... paths) throws IOException {
        for (String path: paths) {
            FileReader fileReader = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = null;
            while((line=bufferedReader.readLine()) != null){
                String[] words = line.split(",");//分割一行的单词
                //存储单词并记录个数
                for(String w: words){
                    int intW = Integer.parseInt(w);
                    number ++;
                    count += intW;
                    if(intW < min){
                        min = intW;
                    }
                    if(intW > max){
                        max = intW;
                    }
                }
            }
            avg = count/number;
        }
        System.out.println("min:"+min+"max:"+max+"avg:"+avg);
    }
}
