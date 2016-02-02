package com.example.spark.eventemiter

import java.io._
import java.net.{Socket, ServerSocket}
import java.text.SimpleDateFormat
import java.util.concurrent.{Executors, ThreadPoolExecutor, Executor}
import java.util.{UUID, Calendar, Date}

import com.rabbitmq.client.{MessageProperties, ConnectionFactory}

import scala.util.Random

trait SocketServer {

  class Sensor(val name: String, max: Int, val socket: Socket) extends Runnable {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    override def run(): Unit = {
      try {
        println(s"sending sensor data for ${name}")
        while (true) {
          val bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
          val sensorData = s"${df.format(new Date())} $name ${getPositiveRandom(max, false).toString}"
          bw.write(sensorData)
          bw.newLine()
          bw.flush()
          Thread.sleep(900)
        }
      } finally {
        socket.close()
      }
    }

    def getPositiveRandom(max: Int, zeroIncluded: Boolean = true): Int = {
      val rand = new Random
      val num = rand.nextInt() % max

      if (!zeroIncluded && num == 0) getPositiveRandom(max, false)
      else if (num < 0)
        num * -1
      else
        num
    }
  }

  def start() = {
    val serverSocket = new ServerSocket(9999)
    val executor = java.util.concurrent.Executors.newCachedThreadPool()
    while (true) {
      val socket = serverSocket.accept()
      val sensors = Seq(new Sensor("Thermostat", 2, socket), new Sensor("Humidity", 333, socket), new Sensor("Voltage", 5, socket))
      sensors.foreach(sensor => executor.execute(sensor))
    }
  }
}

object Emitter extends App {
  val ss = new SocketServer {}
  ss.start()
}

object TestClient {
  def main(args: Array[String]) {
    val socket = new Socket("localhost", 9999)
    val is = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
    var data: Array[Byte] = new Array(80)
    while (true) {
      println(is.readLine())
    }
  }
}


object Test extends App {
  val str = "2016-02-02 09:50:44 Voltage 331"
  val tokens = str.split(" ")
  println(tokens(2))
}