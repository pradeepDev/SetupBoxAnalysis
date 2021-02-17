/*
 * 2. Filter all the record with event_id=101
		i. Get the total number of devices with PowerState="On/Off"
 */

package com.setupbox.KPI2

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object PowerStatus {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D::/software/scala/hadoop-2.5.0-cdh5.3.2")
    System.setProperty("spark.sql.warehouse.dir", "file:/D:/software/scala/spark-2.0.2-bin-hadoop2.6/spark-warehouse")

    val spark = SparkSession.builder.appName("SetBox").master("local").getOrCreate()

    val file = spark.read.textFile("D:\\Project\\SetupBox\\Set_Top_Box_Data.txt").rdd
    val data = file.filter(f => f.contains("^101^"))

    val splitdata = data.map { line =>
      {
        val value = line.split("\\^")
        val xmlVal = value(4)
        val body = XML.loadString(xmlVal)
        var chtype: String = "";
        for (nv <- body.child) {
          val childXml = XML.loadString(nv.toString())
          val name = childXml.attributes("n").toString()
          val nmValue = childXml.attributes("v").toString()
          if (name == "PowerState") {
            chtype = nmValue
          }
        }
        (chtype, 1)
      }
    }.reduceByKey(_ + _)
    val total = splitdata.values.sum()
    splitdata.foreach(println)
    println("\nTotal number of devices with the PowerState=\"On/Off\": " + total)

  }
}