package week2

/**
 * Created by Administrator on 2015/12/22 0022.
 */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object SougouQA{
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SougouQA <file1> <file1>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("SougouQA")
    val sc = new SparkContext(conf)
    sc.textFile(args(0)).map(_.split("\t")).filter(_.length==3).map(x=>(x(1),1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).saveAsTextFile(args(1))


    sc.stop()
  }
}
