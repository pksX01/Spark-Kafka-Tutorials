package com.accen

import org.apache.spark._
import org.apache.spark.streaming._

object StatefulNetworkWordCount {
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = runningCount.getOrElse(0) + newValues.sum
    Some(newCount)
  }

  def main(args: Array[String]) {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local[2]").setAppName("StatefulNetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Set checkpoint directory
    ssc.checkpoint(".")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))

    // Update state using `updateStateByKey`
    val runningCounts = pairs.updateStateByKey[Int](updateFunction _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    runningCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}