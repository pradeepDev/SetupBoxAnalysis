package com.setupbox.KPI3

import org.apache.spark.sql.SparkSession
import scala.xml.XML

object maximumpricegroup {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:/software/scala/hadoop-2.5.0-cdh5.3.2")
    System.setProperty("spark.sql.warehouse.dir", "file:/D:/software/scala/spark-2.0.2-bin-hadoop2.6/spark-warehouse")

    val spark = SparkSession.builder.appName("SetBox").master("local").getOrCreate()

    val file = spark.read.textFile("D:\\Project\\SetupBox\\Set_Top_Box_Data.txt").rdd
    val data = file.filter(f => { f.contains("^102^") || f.contains("^113^") })
    val result = data.map { line =>
      {
        val value = line.split("\\^")
        val body = XML.loadString(value(4))
        var offerId: String = "";
        var price: Double = 0.0
        for (nv <- body.child) {
          val childXml = XML.loadString(nv.toString())

          val name = childXml.attributes("n").toString()
          val nmvalue = childXml.attributes("v")
          if (name == "OfferId") {
            offerId = nmvalue.toString()
          }
          if (name == "Price") {
            if (nmvalue.toString() == null || nmvalue.toString() == "") {
              price = 0.0
            } else {
              price = java.lang.Double.parseDouble(nmvalue.toString())

            }
          }
        }
        (offerId, price)
      }
    }
      .groupByKey()
      .map(f => (f._1, f._2.max))
      .sortBy(f => f._2, false)
    result.foreach(println)
  }
}