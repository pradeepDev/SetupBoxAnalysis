package com.setupbox.KPI6

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object GroupbyButtonName {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:/software/scala/hadoop-2.5.0-cdh5.3.2")
    System.setProperty("spark.sql.warehouse.dir", "file:/D:/software/scala/spark-2.0.2-bin-hadoop2.6/spark-warehouse")

    val spark = SparkSession.builder.appName("SetBox").master("local").getOrCreate()

    val file = spark.read.textFile("D:\\Project\\SetupBox\\Set_Top_Box_Data.txt").rdd
    val data = file.filter(f => f.contains("^107^") )
    val result = data.map { line =>
      {
        val value = line.split("\\^")
        val body = XML.loadString(value(4))
        var buttonName: String = "";
        for (nv <- body.child) {
          val childXml = XML.loadString(nv.toString())
          val name = childXml.attributes("n").toString()
          val nmvalue = childXml.attributes("v")
          if (name == "ButtonName") {
            buttonName = nmvalue.toString()
          }
        }
        (value(5), buttonName)
      }
    }
     .groupByKey()
    result.foreach(println)
  }
}