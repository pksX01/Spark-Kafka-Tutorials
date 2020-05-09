import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SampleKafkaStreaming1 {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("program started")

    val conf = new SparkConf().setMaster("local[2]").setAppName("bhapra")
    val ssc = new StreamingContext(conf, Seconds(5))
val kafkaStream = KafkaUtils.createStream(ssc, "master:2181","spark-streaming-consumer-group", Map("movies" -> 1))
val lines = kafkaStream.map(_._2)
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
wordCounts.print()
ssc.start
ssc.awaitTermination()

  }
}