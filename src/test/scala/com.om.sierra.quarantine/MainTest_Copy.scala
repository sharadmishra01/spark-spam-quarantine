package com.om.sierra.quarantine

//import com.openmarket.sierra.quarantine.Main.enrichColumns
//import com.openmarket.sierra.quarantine.Main.filterQuarantineEvents
import org.scalatest.funsuite.AnyFunSuite

//import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetComparer}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types._
//
class MainTest_Copy
  extends AnyFunSuite
//    with SparkSessionTestWrapper
//    with DatasetComparer
//    with DataFrameComparer{
//
//  test("Successfully filter Spam Quarantine Events") {
//
//    val sourceDF = spark.read
//          .format("json")
//          .option("multiline","true")
//          .load("./src/test/resources/events/AllKafkaEvents.json")
//
//    val actualDF = sourceDF.transform(filterQuarantineEvents)
//
//    val expectedDF = spark.read
//      .format("json")
//      .option("multiline","true")
//      .load("./src/test/resources/events/AllKafkaSpamQuarantineEvents.json")
//
//    assertSmallDatasetEquality(actualDF,expectedDF)
//  }
//
//  test("Successfully select Event Content and Event JSON  ") {
////    [offset, value, topic, timestamp, timestampType, partition, key]
//    import spark.implicits._
//
//    val sourceDF = Seq(
//      "{\"content\":\"value_1\",\"acceptTs\":1,\"type\":\"messageQueued\"}",
//      "{\"content\":\"value_2\",\"acceptTs\":2,\"type\":\"messageDequeued\"}"
//    ).toDF("value")
//
//    val actualDF = sourceDF.transform(enrichColumns)
//
//    val expectedSchema = List(
//      StructField("content", StringType, true),
//      StructField("type", StringType, true),
//      StructField("json", StringType, true)
//    )
//
//    val expectedData = Seq(
//      Row("value_1","messageQueued","{\"content\":\"value_1\",\"acceptTs\":1,\"type\":\"messageQueued\"}"),
//      Row("value_2","messageDequeued","{\"content\":\"value_2\",\"acceptTs\":2,\"type\":\"messageDequeued\"}")
//    )
//
//    val expectedDF = spark.createDataFrame(
//      spark.sparkContext.parallelize(expectedData),
//      StructType(expectedSchema)
//    )
//
//    assertSmallDataFrameEquality(actualDF, expectedDF)
//  }
//
//
//}