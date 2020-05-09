package com.accen;
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object updateStateByKeyWordCount {
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = runningCount.getOrElse(0) + newValues.sum
    Some(newCount)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("updateStateByKeyWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
	ssc.checkpoint("/home/vagrant/bigdata/mystuff/mystreams/")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val runningCounts = pairs.updateStateByKey[Int](updateFunction _)
    runningCounts.print()
    ssc.start() 
    ssc.awaitTermination() 
  }
}
