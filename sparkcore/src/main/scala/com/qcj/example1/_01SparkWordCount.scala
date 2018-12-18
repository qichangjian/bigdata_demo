package com.qcj.example1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取HDFS上数据进行wordCount
  */
/*
  *  报错：
  *      java.lang.IllegalArgumentException: java.net.UnknownHostException: ns1
  *  解决方法：
  *      把hadoop中的两个配置文件hdfs-site.xml和core-site.xml导入resources目录即可
  */
object _01SparkWordCount {
  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf()
    conf.setAppName(s"${_01SparkWordCount.getClass.getSimpleName}")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    //使用hdfs路径需要拷贝配置文件到resources目录下
    //    val linesRDD = sc.textFile("hdfs://bd1807/1.txt")
    val linesRDD = sc.textFile("D:/1.txt")
    val wordsRDD = linesRDD.flatMap(line => line.split("\\s+"))
    val pairsRDD = wordsRDD.map(word => (word, 1))
    val rbkRDD = pairsRDD.reduceByKey((v1, v2) => v1 + v2)
    rbkRDD.foreach(p => println(p._1 + "===>" + p._2))

    sc.stop();*/

    val conf = new SparkConf().setMaster("local[2]").setAppName(s"${_01SparkWordCount.getClass.getSimpleName}")
    val sc = new SparkContext(conf)

    val linesRDD:RDD[String] = sc.textFile("hdfs://bd1807/1.txt")
    linesRDD.flatMap{case line =>{
      val fields = line.split("\\s+")
      fields
    }}.map(line => {
      (line,1)
    }).reduceByKey((v1,v2)=>{
      v1 + v2
    }).foreach(p =>println(s"${p._1}:${p._2}"))
  }
}
