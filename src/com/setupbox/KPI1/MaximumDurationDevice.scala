/*1. Filter all the record with event_id=100
i. Get the top five devices with maximum duration
ii. Get the top five Channels with maximum duration
iii. Total number of devices with ChannelType="LiveTVMediaChannel"*/

package com.setupbox.KPI1

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object MaximumDurationDevice {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D::/software/scala/hadoop-2.5.0-cdh5.3.2")
    System.setProperty("spark.sql.warehouse.dir", "file:/D:/software/scala/spark-2.0.2-bin-hadoop2.6/spark-warehouse")

    val spark = SparkSession.builder.appName("SetBox").master("local").getOrCreate()

    val file = spark.read.textFile("D:\\Project\\SetupBox\\Set_Top_Box_Data.txt").rdd
    val data = file.filter(f => f.contains("^100^"))

    val splitdata = data.map { line =>
      {
        val value = line.split("\\^")
        val deviceId = value(5)
        val xmlVal = value(4)
        val body = XML.loadString(xmlVal)
        var duration = 0L
        for (nv <- body.child) {
          val childXml = XML.loadString(nv.toString())
          val name = childXml.attributes("n").toString()
          val nmValue = childXml.attributes("v").toString()
          if (name == "Duration") {
            duration = java.lang.Long.parseLong(nmValue)
          }
        }

        
        (deviceId, duration)

      }
    }.groupByKey()
      .map(f => (f._1, f._2.max))
      .sortBy(f => f._2, false)
      .take(5)
    splitdata.foreach(println)

  }
}