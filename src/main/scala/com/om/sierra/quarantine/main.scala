package com.om.sierra.quarantine

import java.util.Properties

import com.om.sierra.quarantine.Main.bootstrap_servers
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, get_json_object}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scalaj.http.{Http, HttpOptions}
import spray.json._

object Main extends App {

  val master = "local[*]"
  val bootstrap_servers = "localhost:9092"

  val kafka_group = "kafka_group_1"
  val topics_in = "test"
  val topics_out = "test-written"
  val flask_endpoint = "http://localhost:5001/other/post"
  val typeMessageQueued = "messageQueued"
  val typeMessageDequeued = "messageDequeued"

  val conf = new SparkConf().setAppName("spark-demo").setMaster(master)
  conf.set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext(conf)
  //val bootstrap_servers = sc.broadcast("localhost:9092")

  val ssc = new StreamingContext(conf, Seconds(101))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> bootstrap_servers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> kafka_group,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> "false"
  )

  val input_topics = Array(topics_in)
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](input_topics, kafkaParams)
  )

  stream.map(record => (record.key, record.value)).print();
  stream.foreachRDD { rdd =>

    rdd.foreach { record =>
      val recordValue = record.value()
      try {
        if (recordValue.contains(typeMessageQueued)) {
          val jsonVal = recordValue.parseJson.asJsObject
          val result = Http(flask_endpoint).postData(getContentWithPIIRemoved(jsonVal).toString)
            .header("Content-Type", "application/json")
            .header("Charset", "UTF-8")
            .option(HttpOptions.readTimeout(1000)).asString
          val writeMessageQueuedEvent = parseMessageQueuedEvent(jsonVal, result.body)
          writeToKafka(topics_out, record.key(), writeMessageQueuedEvent.toString, bootstrap_servers)
        } else if (recordValue.contains(typeMessageDequeued)) {
          writeToKafka(topics_out, record.key(), recordValue, bootstrap_servers)
        } else {
          println("Skipping record  " + recordValue + " does not have desired type")
        }
      } catch {
        case ex: Exception => {
          println(" Exception occurred, skipping record " + recordValue + " ----:Cause, " + ex.getMessage)
        }
      }
    }
  }

  ssc.start();
  ssc.awaitTermination()
  println("--terminating process--")

  /**
   * writes to kafka with the given key and string
   *
   * @param topic
   * @param key
   * @param value
   */
  def writeToKafka(topic: String, key: String, value: String, bootstrap_server: String): Unit = {
    val props = new Properties()
    if (bootstrap_server == null){
      println("Could not find boor strap server***************, using default")
      props.put("bootstrap.servers", "10.55.29.6:9092")

    } else {
      props.put("bootstrap.servers", bootstrap_server)

    }
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, key, value)
    producer.send(record)
    producer.close()
  }

  /**
   * calls flask engine with appropriate JSON object creation
   *
   * @param jsonVal
   * @return content
   */
  def getContentWithPIIRemoved(jsonVal: JsObject): JsObject = {
    val list = List("content")
    val content = jsonVal.fields.filterKeys(key => list.contains(key))
    JsObject(content)
  }

  /**
   * returns the required json after response from flask engine
   * and gets the content from response and appends to json
   *
   * @param jsonVal
   * @param response
   * @return event
   */
  def parseMessageQueuedEvent(jsonVal: JsObject, response: String): JsObject = {
    val list = List(
      "messageId",
      "acceptTs",
      "tag",
      "inQueueId",
      "accountId",
      "sourceAddress",
      "destinationAddress",
      "contentEncoding",
      "content",
      "nextAttemptTs",
      "eventType",
      "type",
      "_firstEts",
      "apiVersion")

    response.parseJson.asJsObject.fields.get("content_returned")
    var event = jsonVal.fields.filterKeys(key => list.contains(key))
    event += "contentPIIRemoved" -> response.parseJson.asJsObject.fields.get("content_returned").head;
    JsObject(event)
  }

  ///

  //    val qEventsDf = filterQuarantineEvents(stream)
  //
  //    val qEventContentAndJson = enrichColumns(qEventsDf)
  //
  //    def filterQuarantineEvents(stream : DataFrame) = {
  //      stream.filter(
  //        get_json_object(col("value"), "$.type") === "messageDequeued"
  //          || get_json_object(col("value"), "$.type") === "messageQueued"
  //      )
  //    }
  //
  //    def enrichColumns(df: DataFrame ) = {
  //      df.select(
  //        get_json_object(col("value"), "$.content") as "content",
  //        get_json_object(col("value"), "$.type") as "type",
  //        col("value")
  //      ).withColumnRenamed("value","json")
  //    }
  //    ///

}
