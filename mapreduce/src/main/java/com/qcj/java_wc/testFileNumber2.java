package com.qcj.java_wc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *  6个文件   每个文件中存放的都是数字  每个数字之间的分割符，
 * 		求6个文件中所有的数字的最大值  最小值  平均值
 */
public class testFileNumber2 {
    static float avg=0,max=0,min=1000,count=0;
    static int number=0;

    public static void main(String[] args) throws IOException {
        //获得六个文件中每个文件中单词出现的个数
        Map<Integer, Integer> map1 = countOneFileWord("D:\\11.txt");
        Map<Integer, Integer> map2 = countOneFileWord("D:\\12.txt");
        Map<Integer, Integer> map3 = countOneFileWord("D:\\13.txt");
        Map<Integer, Integer> map4 = countOneFileWord("D:\\14.txt");
        Map<Integer, Integer> map5 = countOneFileWord("D:\\15.txt");
        Map<Integer, Integer> map6 = countOneFileWord("D:\\16.txt");
        Map<Integer, Integer> map = sumAllMap(map1, map2, map3, map4, map5, map6);
        //得到最大值，最小值，平均值
        getResult(map);

    }

    public static Map<Integer,Integer> countOneFileWord(String path) throws IOException {
        FileReader fileReader = new FileReader(path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        Map<Integer,Integer> map = new HashMap<Integer, Integer>();
        String line = null;
        while((line=bufferedReader.readLine()) != null){
            String[] words = line.split(",");//分割一行的单词
            //存储单词并记录个数
            for(String w: words){
                int intW = Integer.parseInt(w);
                //map集合中是否存在
                if(!map.containsKey(w)){
                    map.put(intW,1);
                }else{
                    //个数+1
                    map.put(intW,map.get(w) + 1);
                }
            }
        }
        return map;
    }
    /**
     * 将所有文件中的单词个数汇总到一个map
     */
    public static Map<Integer,Integer> sumAllMap(Map<Integer,Integer> ... maps){
        Map<Integer,Integer> sumMap = new HashMap<Integer, Integer>();
        //遍历map参数
        for (Map<Integer,Integer> map:maps) {
            //迭代单个map
            Set<Map.Entry<Integer, Integer>> entrySet = map.entrySet();
            for (Map.Entry<Integer,Integer> entry:entrySet) {
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

    public static void getResult(Map<Integer,Integer> map){
        String[] result = new String[3];
        Set<Map.Entry<Integer, Integer>> entrySet = map.entrySet();
        for (Map.Entry<Integer,Integer> entry:entrySet) {
            Integer key = entry.getKey();
            Integer value = entry.getValue();
            number += value;
            count += key*value;
            if(key < min){
                min = key;
            }
            if(key > max){
                max = key;
            }
        }
        avg = count/number;
        System.out.println("min:"+min+"max:"+max+"avg:"+avg);
    }
}
