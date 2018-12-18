package com.qcj.example2_rdd_operator

import com.qcj.SparkUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
    action算子：
    1、reduce：
        执行reduce操作，返回值是一个标量
    2、collect： 慎用
        将数据从集群中的worker上拉取到dirver中，所以在使用的过程中药慎用，意外拉取数据过大造成driver内存溢出OOM(OutOfMemory)
        NPE(NullPointerException)
        所以在使用的使用，尽量使用take，或者进行filter再拉取
    3、count：返回当前rdd中有多少条记录
        select count(*) from tbl;
    4、take：take(n)
        获取rdd中前n条记录
        如果rdd数据有序，可以通过take(n)求TopN
    4. top 函数用于从RDD中，按照默认（降序）或者指定的排序规则，返回前num个元素。
    4.takeOrdered和top类似，只不过以和top相反的顺序返回元素
    5、first:take(1)
        获取rdd中的第一条记录
    6、saveAsTextFile：
        将rdd中的数据保存到文件系统中
    7、countByKey：和reduceByKey效果相同，但reduceByKey是一个Transformation
        统计相同可以出现的次数，返回值为Map[String, Long]
        countByKey作用就是记录每一个key出现的次数，作用同reduceByKey(_+_)
    8、foreach：略 遍历

    9.保存
    saveAsTextFile
    saveAsNewAPIHadoopFile
    saveAsObjectFile

 */
/**
  *App特质：最好不要用
  * trait App指的是scala.App，我们的单例对象混入这个特质就可以运行，而不必进行Main方法的定义
  */
object _04RddActionOps {
  def main(args: Array[String]): Unit = {
    SparkUtils.ClosePrintLogger
    val conf = new SparkConf().setMaster("local[2]").setAppName(s"${_04RddActionOps.getClass.getSimpleName}")
    val sc = new SparkContext(conf)
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

    val listRDD:RDD[String] = sc.parallelize(list)
    val listRDD1 = sc.parallelize(1 to 9)

    //1.reduce:统计rdd中的和
    val 和 = listRDD1.reduce((v1,v2)=>{v1+v2})
    println(和)

    //2.count个数 慎用 将数据从集群中的worker上拉取到dirver中，所以在使用的过程中药慎用，意外拉取数据过大造成driver内存溢出OOM(OutOfMemory)
    println(s"${listRDD1.count()}")

    //3.take获取rdd中的n条记录
    val take:Array[Int] = listRDD1.take(3)
    println(take.mkString("[",",","]"))

    //4.求第一条记录
    val first = listRDD1.first()
    println(first)

    //5.top函数用于从RDD中，按照默认（降序）或者指定的排序规则，返回前num个元素。
    println(listRDD1.top(4).mkString("[",",","]"))
    //6.takeOrdered和top类似，只不过以和top相反的顺序返回元素
    println(listRDD1.takeOrdered(4).mkString("[",",","]"))

    val top3 = listRDD1.takeOrdered(3)(new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = {
        y.compareTo(x)
      }
    })
    println(top3.mkString("[",",","]"))


    println("---------------saveAsTextFile---------------------")
    //  listRDD.saveAsTextFile("D:/parkout")//注意这里将resource中的xml注释
    //  listRDD.saveAsTextFile("data/day2/parkout")//这个是输出到当前项目data路径下
    //  listRDD.saveAsObjectFile("data/day2/parkout2")//以对象形式存储

    println("---------------saveAsHadoop/SequenceFile---------------------")
    val cid2InfoRDD:RDD[(String, String)] = listRDD.map{case line => {
      val fields = line.split("\\s+")
      (fields(2), line)
    }}
    /**
      * saveAsHadoopFile()  ---> org.apache.hadoop.mapred.OutputFormat 接口
      * saveAsNewAPIHadoopFile() --> org.apache.hadoop.mapreduce.OutputFormat 抽象类
      * job.setOutputFormat(xxx.classs)
      *
      *
      *  path: String   --->将rdd数据存储的目的地址
         keyClass: Class[_], mr中输出记录包含key和value，需要指定对应的class
         valueClass: Class[_],
         job.setOutputKeyClass()
         job.setOutputValueClass()
         outputFormatClass: Class[_ < NewOutputFormat[_, _]] 指定对应的Format来完成数据格式化输出
         TextOutputFormat
      */
    //换成集群地址就行了
    cid2InfoRDD.saveAsNewAPIHadoopFile(
      "data/day2/parkout3",
      classOf[Text],
      classOf[Text],
      classOf[TextOutputFormat[Text, Text]]
    )

    //countByKey作用就是记录每一个key出现的次数，作用同reduceByKey(_+_)
    val cid2Count: collection.Map[String, Long]=cid2InfoRDD.countByKey()
    for ((k,v)<-cid2Count){
      println(k+"---"+v)
    }

    sc.stop()
  }
}
