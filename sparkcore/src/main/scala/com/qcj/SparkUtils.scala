package com.qcj

import org.apache.log4j.{Level, Logger}


/**
  * spark工具类
  */
object SparkUtils {

  def ClosePrintLogger = {
    //设置日志打印级别log
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)
  }

}
