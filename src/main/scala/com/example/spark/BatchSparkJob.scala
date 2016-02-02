package com.example.spark

import org.apache.spark.{SparkContext, SparkConf}

object BatchSparkJob {
  def main(args: Array[String]) {
    val logFile = "/home/sajith/scratch/spark-test/big.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val file = sc.textFile(logFile)
    val words = file.map(line => {
      val tokens = line.split(" ")
      (tokens(2), tokens(3))
    })
    val wc = words.reduceByKey((a: String, b: String) => (a.toInt + b.toInt).toString)
    wc.foreach(println)
  }
}
