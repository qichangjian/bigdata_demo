package com.qcj.example1

import org.apache.spark.sql.SparkSession

/**
  * java.lang.IllegalArgumentException: java.net.UnknownHostException: ns1
  * 把hadoop中的两个配置文件hdfs-site.xml和core-site.xml导入resources目录即可
  * Read读
  * Eva求值
  * Print打印
  * Loop循环 再来一遍
  * repl 交互式查询
  *
  */
object _03SparkWordCount {
  def main(args: Array[String]): Unit = {
    if(args == null || args.length < 1){
      println(
        """Parameter Errors!Usage:<inputpath>
          |inputpath :  程序数据输入源
        """.stripMargin)

      val Array(inputpath) = args

      val spark = SparkSession
        .builder()
        .appName(s"${_03SparkWordCount.getClass.getSimpleName}")
        .getOrCreate()
      val sc = spark.sparkContext

      val linesRDD = sc.textFile(inputpath)

      linesRDD.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
        .collect()//将集群中的rdd对应的partition中的数据拉去到driver中，在工作中慎用
        .foreach(println)
    }
  }
}
