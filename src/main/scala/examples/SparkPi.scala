package examples

import scala.math.random

import org.apache.spark._

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[4]")
    //conf.set("master","spark://moon:7077")
   // conf.setMaster("spark://chaoren:7077").setJars(List("E:\\cloud\\ideaworkspace\\sparkTest1\\out\\artifacts\\sparktest1_jar\\sparktest1.jar") )
    conf.set("spark.cores.max","1")//
    conf.set("spark.executor.memory", "1g")
    val spark = new SparkContext(conf)

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices

    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)

    spark.stop()
  }
}