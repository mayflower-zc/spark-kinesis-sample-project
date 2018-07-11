import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.Latest
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object KinesisWordCountASL {
  def main(args: Array[String]): Unit = {
    //调整日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val appName = "WordCountsApp"
    //Kinesis Stream 名称
    val streamName = "word-counts-kinesis"
    val endpointUrl = "https://kinesis.us-east-1.amazonaws.com"

    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials !=null, "No AWS credentials found. Please specify credentials using one of the methods specified " +
      "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription.getShards().size()

    val numStreams = numShards

    // Spark Streaming batch interval
    val batchInterval = Milliseconds(2000)
    val kinesisCheckpointInterval = batchInterval
    val regionName = getRegionNameByEndpoint(endpointUrl)

    val sparkConf = new SparkConf().setAppName("KinesisWordCountASL").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, batchInterval)
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(streamName)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .initialPosition(new Latest())
        .checkpointAppName(appName)
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }

    //Union all the streams
    val unionStreams = ssc.union(kinesisStreams)

    val words = unionStreams.flatMap(byteArray => new String(byteArray).split(" "))

    val wordCounts =words.map(word => (word, 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def getRegionNameByEndpoint(endpoint: String): String = {
    import scala.collection.JavaConverters._
    val uri = new java.net.URI(endpoint)
    RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
      .asScala
      .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
      .map(_.getName)
      .getOrElse(
        throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
  }
}
