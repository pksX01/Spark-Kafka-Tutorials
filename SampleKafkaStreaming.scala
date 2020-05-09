import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SampleKafkaStreaming {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("program started")

    val conf = new SparkConf().setMaster("local[2]").setAppName("bhapra")
    val ssc = new StreamingContext(conf, Seconds(5))

    // my kafka topic name is 'test'
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("movies" -> 1))

    kafkaStream.print()
    ssc.start
    ssc.awaitTermination()

  }
}