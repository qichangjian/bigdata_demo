package com.qcj.java_wc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * java实现  wordcount
 * 		6个文件  每个文件存放都是单词  单词之间的分割符 \t
 * 		统计这6个文件中每一个单词出现的总次数
 * 		1.txt   a---2
 * 		2.txt   a---3
 * 		3.txt   a---0
 * 		4.txt   a----6
 * 		5.txt    a----7
 * 		6.txt   a---0
 * 				a-----18
 * 			C:\Users\Administrator\IdeaProjects\mapreducehelloworld\src\main\java\com\qcj\testWordCount.java
 */
public class testWordCount {

    public static void main(String[] args) throws IOException {
        //获得六个文件中每个文件中单词出现的个数
        Map<String, Integer> map1 = countOneFileWord("D:\\1.txt");
        Map<String, Integer> map2 = countOneFileWord("D:\\2.txt");
        Map<String, Integer> map3 = countOneFileWord("D:\\3.txt");
        Map<String, Integer> map4 = countOneFileWord("D:\\4.txt");
        Map<String, Integer> map5 = countOneFileWord("D:\\5.txt");
        Map<String, Integer> map6 = countOneFileWord("D:\\6.txt");
        System.out.println(map1);
        System.out.println(map2);
        System.out.println(map3);
        System.out.println(map4);
        System.out.println(map5);
        System.out.println(map6);

        //求和
        Map<String, Integer> map = sumAllMap(map1, map2, map3, map4, map5, map6);
        System.out.println(map);
    }

    /**
     * 统计单个文件中 单词出现的次数
     *
     * 逐行读取文件中内容，通过\t（制表符）分割，获取每个单词和单词出现的次数
     * 存放在Map<单词，个数>中
     */
    public static Map<String,Integer> countOneFileWord(String path) throws IOException {
        FileReader fileReader = new FileReader(path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        Map<String,Integer> map = new HashMap<String, Integer>();
        String line = null;
        while((line=bufferedReader.readLine()) != null){
            String[] words = line.split("\t");//分割一行的单词
            //存储单词并记录个数
            for(String w: words){
                //map集合中是否存在
                if(!map.containsKey(w)){
                    map.put(w,1);
                }else{
                    //个数+1
                    map.put(w,map.get(w) + 1);
                }
            }
        }
        return map;
    }

    /**
     * 将所有文件中的单词个数汇总到一个map
     */
    public static Map<String,Integer> sumAllMap(Map<String,Integer> ... maps){
        Map<String,Integer> sumMap = new HashMap<String, Integer>();
        //遍历map参数
        for (Map<String,Integer> map:maps) {
            //迭代单个map
            Set<Entry<String, Integer>> entrySet = map.entrySet();
            for (Entry<String,Integer> entry:entrySet) {
                //是否存在于总map中
                if(!sumMap.containsKey(entry.getKey())){
                    sumMap.put(entry.getKey(),entry.getValue());
                }else{
                    sumMap.put(entry.getKey(),sumMap.get(entry.getKey()) + entry.getValue());
                }
            }
        }
        return sumMap;
    }
}
