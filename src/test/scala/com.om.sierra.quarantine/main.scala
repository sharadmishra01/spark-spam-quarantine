//import com.openmarket.sierra.quarantine.Main.enrichColumns
//import com.openmarket.sierra.quarantine.Main.filterQuarantineEvents
import org.scalatest.funsuite.AnyFunSuite
//import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetComparer}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types._
import spray.json._
import com.om.sierra.quarantine.Main
class main
  extends AnyFunSuite {

  test("Preparing of JSON content for flask servre post")
  {
    val message = "{\"destination\":\"124\",\"source\":\"" +
      "55565\",\"productId\":\"12\",\"submittedTs\":\"44323\",\"userAgent\"" +
      ":\"v7\",\"_firstEts\":\"2022\",\"content\":\"Blah blah food\",\"accountId\"" +
      ":\"111-222\",\"contentEncoding\":\"NORMAL\",\"channel\":\"xxc\",\"type\":\"messageQueued\"}"
    val jsonVal = message.parseJson.asJsObject
    val output = Main.getContentWithPIIRemoved(jsonVal)
    assert(output.fields.get("content").head == JsString("Blah blah food") && output.fields.size ==1)

  }

  test("Parse Message Queued Event")
  {
    val message = "{\"destination\":\"124\",\"source\":\"" +
      "55565\",\"productId\":\"12\",\"submittedTs\":\"44323\",\"userAgent\"" +
      ":\"v7\",\"_firstEts\":\"2022\",\"content\":\"Blah blah food\",\"accountId\"" +
      ":\"111-222\",\"contentEncoding\":\"NORMAL\",\"channel\":\"xxc\",\"type\":\"messageQueued\"}"
    val jsonVal = message.parseJson.asJsObject
    val output = Main.parseMessageQueuedEvent(jsonVal, "{\"content_returned\" : \"check\"}")
    assert(output.fields.get("contentPIIRemoved").head == JsString("check") && output.fields.size == 6)

  }

  test("Write to Kafka")
  {
    val message = "{\"destination\":\"124\",\"source\":\"" +
      "55565\",\"productId\":\"12\",\"submittedTs\":\"44323\",\"userAgent\"" +
      ":\"v7\",\"_firstEts\":\"2022\",\"content\":\"Blah blah food\",\"accountId\"" +
      ":\"111-222\",\"contentEncoding\":\"NORMAL\",\"channel\":\"xxc\",\"type\":\"messageQueued\"}"
    val result = Main.writeToKafka("topic-written", "sampleKey", message, "localhost:9092")
    assert(result != null)

  }
}