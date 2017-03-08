/**
  * Created by zjkgf on 2017/3/8.
  */
import java.io.File

import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}
object ScalaMain {
  def wordcount(): Unit ={
    val outputpath = "e:/output/"
    DeleteDirectory.deleteDir(new File(outputpath))
    println( "Hello World!" )
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val line = sc.textFile("e:/notice.txt")
    val c = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    c.saveAsTextFile(outputpath)
    sc.stop()
  }
  def calculatepi(): Unit ={
    val slices = 16
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val spark = new SparkContext(conf)
    val n = 1000000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
  def main(args: Array[String]): Unit = {
    wordcount()
    calculatepi()
  }

}