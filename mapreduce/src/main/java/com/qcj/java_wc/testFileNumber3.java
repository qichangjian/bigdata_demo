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
public class testFileNumber3 {

    public static void main(String[] args) throws IOException {
        //获得六个文件中每个文件中单词出现的个数
        Map<String, Integer> map1 = map("D:\\11.txt");
        Map<String, Integer> map2 = map("D:\\12.txt");
        Map<String, Integer> map3 = map("D:\\13.txt");
        Map<String, Integer> map4 = map("D:\\14.txt");
        Map<String, Integer> map5 = map("D:\\15.txt");
        Map<String, Integer> map6 = map("D:\\16.txt");
        Map<String, Integer> map = merge(map1, map2, map3, map4, map5, map6);
        //得到最大值，最小值，          平均值:这个要另外写程序
        System.out.println(map);

    }

    public static Map<String,Integer> map(String path) throws IOException {
        //int avg=0;
        int number=0,max=0,min=1000,count=0;
        FileReader fileReader = new FileReader(path);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        Map<String,Integer> map = new HashMap<String, Integer>();
        String line = null;
        while((line=bufferedReader.readLine()) != null){
            String[] words = line.split(",");//分割一行的单词
            //存储单词并记录个数
            for(String w: words){
                int intW = Integer.parseInt(w);
                //number ++;
                //count += intW;
                if(intW < min){
                    min = intW;
                }
                if(intW > max){
                    max = intW;
                }
            }
            //avg = count/number;
            map.put("最小值",min);
            map.put("最大值",max);
            //map.put("平均值",avg);
        }
        return map;
    }
    /**
     * 将所有文件中汇总到一个map
     */
    public static Map<String,Integer> merge(Map<String,Integer> ... maps){
        Map<String,Integer> sumMap = new HashMap<String, Integer>();
        //遍历map参数
        for (Map<String,Integer> map:maps) {
            //迭代单个map
            Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
            for (Map.Entry<String,Integer> entry:entrySet) {
                String key = entry.getKey();
                Integer value = entry.getValue();
                //是否存在于总map中
                if(!sumMap.containsKey(key)){
                    sumMap.put(key,value);
                }else{
                    if(key.equals("最大值")){
                        if(sumMap.get(key) < value){
                            sumMap.put(key,value);
                        }
                    }
                    if(key.equals("最小值")){
                        if(sumMap.get(key) > value){
                            sumMap.put(key,value);
                        }
                    }
                }
            }
        }
        return sumMap;
    }

}
