import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import spark.sql

val topics = Array("tst-topic")

val streamingContext = new StreamingContext(sc, Seconds(5))

val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "172.31.36.197:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "test_group",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val topics = Array("tst-topic")

val stream = KafkaUtils.createDirectStream[String, String](
  streamingContext,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)
val dest_dir = "s3://datalake01-raw/kfk_test"

val stream = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
stream.foreachRDD { rddRaw =>
    val offsetRanges = rddRaw.asInstanceOf[HasOffsetRanges].offsetRanges

    if (!rddRaw.isEmpty()) {
    
        val rdd = rddRaw.map(_.value.toString)
        val dfOperation = spark.read.json(rdd)
        
        
        dfOperation.coalesce(1).write.mode("append").parquet(dest_dir)  

    }

    //stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
}

streamingContext.start()
streamingContext.awaitTermination()
