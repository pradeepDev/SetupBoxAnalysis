package com.setupbox.KPI4

import org.apache.spark.sql.SparkSession
import scala.xml.XML
import java.lang.Long

object MinAndMaxDuration {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D::/software/scala/hadoop-2.5.0-cdh5.3.2")
    System.setProperty("spark.sql.warehouse.dir", "file:/D:/software/scala/spark-2.0.2-bin-hadoop2.6/spark-warehouse")

    val spark = SparkSession.builder.appName("SetBox").master("local").getOrCreate()

    val file = spark.read.textFile("D:\\Project\\SetupBox\\Set_Top_Box_Data.txt").rdd
    val data = file.filter(f => f.contains("^118^"))

    val result = data.map { line =>
      {
        val value = line.split("\\^")
        val body = XML.loadString(value(4))
        var duration = 0L
        for (nv <- body.child) {
          val chilXml = XML.loadString(nv.toString())
          val name = chilXml.attributes("n").toString()
          val nmValue = chilXml.attributes("v")
          if (name == "DurationSecs") {
          duration = Long.parseLong(nmValue.toString())
         
          }
        }
        (duration)
      }
    }
println("min duration : "+result.min() )
println("\nmax duration : "+result.max() )
  }
}