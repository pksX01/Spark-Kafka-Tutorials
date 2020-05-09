package com.accen;
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCountWindow {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("NetworkWordCountWindow")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

   
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val windowedWordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30),Seconds(10))
    windowedWordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

