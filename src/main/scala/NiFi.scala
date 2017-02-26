
import java.nio.charset.StandardCharsets

import org.apache.nifi.remote.client.SiteToSiteClient
import org.apache.nifi.spark.NiFiReceiver
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

/**
  * Created by oleg.baydakov on 26/02/2017.
  */
object NiFi {
  def main(args: Array[String]): Unit = {
    val conf = new SiteToSiteClient
    .Builder()
      .url("http://localhost:8080/nifi/")
      .portName("Spark_Test")
      .buildConfig()


    val config = new SparkConf().setMaster("local[*]") setAppName ("Nifi_Spark_Data")
    val sc = new SparkContext(config)
    val ssc = new StreamingContext(sc, Seconds(10))

    val lines = ssc.receiverStream(new NiFiReceiver(conf, StorageLevel.MEMORY_ONLY))

    val text = lines.map(dataPacket => new String(dataPacket.getContent, StandardCharsets.UTF_8))

    text.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
