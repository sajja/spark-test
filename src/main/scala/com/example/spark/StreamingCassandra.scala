package com.example.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.writer.{StaticWriteOptionValue, TTLOption, WriteConf}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingSparkJob {
  val cal = Calendar.getInstance()
  val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def date() = {
    cal.setTime(new Date)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTime
  }

  def main(args: Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "dev.localhost").setMaster("local[4]").setAppName("Simple Application")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.map(line => {
      val tokens = line.split(" ")
      (tokens(2), tokens(3).toInt)
    })

    val wc = words.reduceByKey(_ + _)

    val x = wc.map((tuple: (String, Int)) => (tuple._1, date(), tuple._2))
    x.saveToCassandra("statistics", "sensor", SomeColumns("name", "date", "value"),writeConf = WriteConf(ttl=new TTLOption(new StaticWriteOptionValue[Int](60))))
    ssc.start()
    ssc.awaitTermination()
  }
}



