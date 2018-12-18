package com.qcj.example1

import org.apache.spark.sql.SparkSession

/**
  * SparkSession是Spark 2.0引如的新概念。SparkSession为用户提供了统一的切入点，来让用户学习spark的各项功能。
  *   在spark的早期版本中，SparkContext是spark的主要切入点，由于RDD是主要的API，我们通过sparkcontext来创建和操作RDD。
  *     对于每个其他的API，我们需要使用不同的context。
  *           例如:
  *           对于Streming，我们需要使用StreamingContext；
  *           对于sql，使用sqlContext；
  *           对于Hive，使用hiveContext。
  *       但是随着DataSet和DataFrame的API逐渐成为标准的API，就需要为他们建立接入点。
  *       所以在spark2.0中，引入SparkSession作为DataSet和DataFrame API的切入点，
  *       SparkSession封装了SparkConf、SparkContext和SQLContext。
  *       为了向后兼容，SQLContext和HiveContext也被保存下来。
  * 　　
  * 　　SparkSession实质上是SQLContext和HiveContext的组合（未来可能还会加上StreamingContext），
  *     所以在SQLContext和HiveContext上可用的API在SparkSession上同样是可以使用的。
  *     SparkSession内部封装了sparkContext，所以计算实际上是由sparkContext完成的。
  */
object _02SparkWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(s"${_02SparkWordCount.getClass.getSimpleName}").master("local").getOrCreate()
    val sc = spark.sparkContext

    val linesRDD = sc.textFile("hdfs://bd1807/1.txt")
    linesRDD.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).foreach(println)

    spark.stop()
  }
}
