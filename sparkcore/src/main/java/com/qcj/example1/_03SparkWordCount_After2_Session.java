package com.qcj.example1;

import org.apache.spark.sql.SparkSession;

/**
 * spark2.x之后：
 *      需要构建SparkSession，使用sparksession来进行构建sparkcontext
 *      这个sparksession管理了sparkcontext，sqlcontext等等的创建。
 */
public class _03SparkWordCount_After2_Session {
    public static void main(String[] args) {
        SparkSession.builder().appName(_03SparkWordCount_After2_Session.class.getSimpleName()).master("local[2]");
    }
}
