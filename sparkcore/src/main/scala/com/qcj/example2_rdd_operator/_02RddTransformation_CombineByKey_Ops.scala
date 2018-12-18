package com.qcj.example2_rdd_operator

import com.qcj.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * 使用combineByKey和aggregateByKey来模拟groupByKey和reduceByKey
  * 不管combineByKey还是aggregateByKey底层都是使用combineByKeyWithClassTag来实现的
  */
/*
  * 使用combineByKey来模拟groupByKey和reduceByKey
  */
object _02RddTransformation_CombineByKey_Ops {

  def main(args: Array[String]): Unit = {
    SparkUtils.ClosePrintLogger

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(s"${_02RddTransformation_CombineByKey_Ops.getClass.getSimpleName}")
      .getOrCreate()

    val sc = spark.sparkContext

    val list = List(
      "5  刘帆 1807bd-xa",
      "2  王佳豪 1807bd-bj",
      "8  邢宏 1807bd-xa",
      "3  刘鹰 1807bd-sz",
      "4  宋志华 1807bd-wh",
      "1  郑祥楷 1807bd-bj",
      "7  张雨 1807bd-bj",
      "6  何昱 1807bd-xa"
    )

    cbk2gbk(sc,list)
    cbk2rbk(sc,list)

    sc.stop()
  }

  /**
    * 用combineByKey实现 groupByKey
    */
  def cbk2gbk(sc: SparkContext, list: List[String]) = {
    val listRDD = sc.parallelize(list)
    println(s"分区个数：${listRDD.getNumPartitions}")
    val cbk2Info:RDD[(String,String)] = listRDD.map { case line => {
      val fields = line.split("\\s+")
      (fields(2), line)
    }}
    //直接使用groupByKey
    println("-----------直接使用groupByKey--------------")
    cbk2Info.groupByKey().foreach(println)

    //combineByKey实现 groupByKey
    println("-----------combineByKey实现 groupByKey:[方式一]--------------")
    val cbk2gbk1 = cbk2Info.combineByKey(createCombiner, mergeValue, mergeCombiners)
    cbk2gbk1.foreach(println)

    println("-----------combineByKey实现 groupByKey:[方式二]--------------")
    val cbk2gbk2:RDD[(String,ArrayBuffer[String])] = cbk2Info.combineByKey(
      line => ArrayBuffer[String](line),//v=>c V是被聚合的rdd中k-v键值对中的value的类型 C是经过聚合操作之后又v转化成的类型
      (ab: ArrayBuffer[String], line: String) => {
        ab.append(line)
        ab
      },
      (ab1: ArrayBuffer[String], ab2: ArrayBuffer[String]) => {
        ab1.appendAll(ab2)
        ab1
      }
    )
    cbk2gbk2.foreach(println)

  }

  /**
    * 用combineByKey实现 reduceByKey
    */
  def cbk2rbk(sc: SparkContext, list: List[String]) = {
    val listRDD = sc.parallelize(list)
    println(s"${listRDD.getNumPartitions}")
    val cbk2Info:RDD[(String,String)] = listRDD.map { case line => {
      val fields = line.split("\\s+")
      (fields(2), line)
    }}
    //直接使用reduceByKey
    println("-----------直接使用reduceByKey--------------")
    //cbk2Info.reduceByKey(_+_).foreach(println)
    cbk2Info.reduceByKey((v1,v2)=>v1+v2).foreach(println)

    //combineByKey实现reduceByKey
    println("-----------combineByKey实现 reduceByKey:[方式一]--------------")
    cbk2Info.combineByKey(
      (line=>ArrayBuffer[String](line)),
      (ab:ArrayBuffer[String],line:String)=>{
        ab.append(line)
        ab
      },
      (ab1:ArrayBuffer[String],ab2:ArrayBuffer[String])=>{
        ab1.appendAll(ab2)
        ab1
      }
    ).foreach(println)
  }


  /**
    * V是被聚合的rdd中k-v键值对中的value的类型
    * C是经过聚合操作之后又v转化成的类型
    * 当前方法，rdd中一个key，在一个分区中，只会创建/调用一次，做数据类型初始化
    * 比如：hello这个key，在partition0和partition1中都有出现
    * 在partition0中聚合的时候createCombiner之被调用一次
    *
    *自己解析：统一个分区中的不同的key只会调用一次这个方法。
    *  例如：第一个分区中有 a b c a四个key
    *     其中key=a是重复的：
    *       第一次a加载调用一次这个方法，
    *       第二个a就会调用下边的 mergeValue方法，
    *       而不会再次调用这个方法
    *
    * (1807bd-wh,4  宋志华 1807bd-wh)
    * (1807bd-bj,1  郑祥楷 1807bd-bj)
    * (1807bd-bj,7  张雨 1807bd-bj)
    * (1807bd-xa,6  何昱 1807bd-xa)
    * (1807bd-xa,5  刘帆 1807bd-xa)
    * (1807bd-bj,2  王佳豪 1807bd-bj)
    * (1807bd-xa,8  邢宏 1807bd-xa)
    * (1807bd-sz,3  刘鹰 1807bd-sz)
    *
    * -----createCombiner---> 1807bd-xa
    * -----createCombiner---> 1807bd-bj
    * >>>-----mergeValue---> 1807bd-xa
    * -----createCombiner---> 1807bd-sz
    * -----createCombiner---> 1807bd-wh
    * -----createCombiner---> 1807bd-bj
    * >>>-----mergeValue---> 1807bd-bj
    * -----createCombiner---> 1807bd-xa
    * >>>-----mergeCombiners--->>> sum1: 1--->sum2: 2
    * >>>-----mergeCombiners--->>> sum1: 2--->sum2: 1
    */
  def createCombiner(line:String):Int = {
    val fields = line.split("\\s+")
    println("-----createCombiner---> " + fields(2))
    1
  }

  /**
    * scala代码，求1+。。。+10
    * var sum = 0
    * for(i <- 1 to 10) {
    *     sum = sum + i
    * }
    * println(sum)
    * mergeValue就类似于上面的代码
    * 合并同一个分区中，相同key对应的value数据，在一个分区中，相同的key会被调用多次
    */
  def mergeValue(sum:Int, line:String):Int = {
    val fields = line.split("\\s+")
    println(">>>-----mergeValue---> " + fields(2))
    sum + 1
  }

  /**
    * 相同key对应分区间的数据进行合并
    */
  def mergeCombiners(sum1:Int, sum2:Int):Int = {
    println(">>>-----mergeCombiners--->>> sum1: " + sum1 + "--->sum2: " + sum2)
    sum1 + sum2
  }
}
