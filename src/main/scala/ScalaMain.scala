/**
  * Created by zjkgf on 2017/3/8.
  */
import java.io.File

import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
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
  var simpledata : ArrayBuffer[Long] = ArrayBuffer[Long]()
  def simpleadd(): Unit ={
    val outputpath = "e:/output/"
    val outputpath2 = "e:/output2/"
    DeleteDirectory.deleteDir(new File(outputpath))
    DeleteDirectory.deleteDir(new File(outputpath2))
    val maxnum = 20000000
    for (i <- 1 to maxnum){
      simpledata += Random.nextInt(200000)
    }
    println("Test begin")
    val begintime1 = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val spark = new SparkContext(conf)
    var distdata = spark.parallelize(simpledata)

//    val result = distdata.map((_,1)).reduceByKey(_+_)
//    result.saveAsTextFile(outputpath)

//    val result = distdata.reduce((a,b) => Math.max(a,b))
//    println(result)

//    val result = distdata.map(a => 3*a).reduce((a,b) => Math.max(a,b))
//    println(result)

//    val result = distdata.map(a => {if (a>100000) 1 else 0 }).reduce((a,b) => a+b)
//    println(result)

    val result = distdata.map((_,1)).reduceByKey((a,b) => a+b)
    result.saveAsTextFile(outputpath)
    val result2 = result.filter(a => {
      if (a._1 < 50000) true else false
    })
    result2.saveAsTextFile(outputpath2)

    val endtime1 = System.currentTimeMillis()
    println("time = "+(endtime1-begintime1))

  }
  def main(args: Array[String]): Unit = {
    wordcount()
    calculatepi()
    simpleadd()
  }

}