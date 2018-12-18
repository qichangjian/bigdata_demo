package com.qcj.example1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 使用lambda表达式WordCount
 * spark2.x之前：需要手动new SparkContext
 *
 * 这是本地方式运行，导入xml会报错
 */
public class _02SparkWordCount_Before2_Scala {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName(_02SparkWordCount_Before2_Scala.class.getSimpleName());
        conf.setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> linesRDD = jsc.textFile("d:\\1.txt");

        JavaRDD<String> wordsRDD = linesRDD.flatMap(line -> {
            String[] fields = line.split("\\s+");
            return Arrays.asList(fields).iterator();
        });
        JavaPairRDD<String,Integer> pairRDD = wordsRDD.mapToPair(word -> {
            return new Tuple2<String,Integer>(word,1);
        });
        JavaPairRDD<String, Integer> rbkRDD = pairRDD.reduceByKey((v1, v2) -> {
            return v1 + v2;
        });
        rbkRDD.foreach(pair -> {
            System.out.println(pair._1+":"+pair._2);
        });

        jsc.close();
    }
}
