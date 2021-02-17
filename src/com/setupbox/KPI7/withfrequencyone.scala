package com.setupbox.KPI7

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object withfrequencyone {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:/software/scala/hadoop-2.5.0-cdh5.3.2")
    System.setProperty("spark.sql.warehouse.dir", "file:/D:/software/scala/spark-2.0.2-bin-hadoop2.6/spark-warehouse")

    val spark = SparkSession.builder.appName("SetBox").master("local").getOrCreate()

    val file = spark.read.textFile("D:\\Project\\SetupBox\\Set_Top_Box_Data.txt").rdd
    val data = file.filter(f => { f.contains("^118^")  })
    val result = data.map { line =>
      {
        val value = line.split("\\^")
        val body = XML.loadString(value(4))
        var frequency: String = ""
        var duration = 0L
        for (nv <- body.child) {
          val childXml = XML.loadString(nv.toString())
          val name = childXml.attributes("n").toString()
          //println(name)
          val nmvalue = childXml.attributes("v")
          if (name == "Frequency") {
            frequency = nmvalue.toString()
            //println(frequency)
          }
        }
        (value(5), frequency)
      }
    }.filter(f => f._2.contains("Once"))
    .groupByKey()
    result.foreach(println)
    
    println("total number of devices with frequency once :"+result.count())
  }
}