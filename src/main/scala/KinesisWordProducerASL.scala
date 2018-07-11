import java.nio.ByteBuffer

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.log4j.{Level, Logger}

import scala.util.Random

object KinesisWordProducerASL {

  def main(args: Array[String]): Unit = {
    //调整日志级别
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //Kinesis Stream 名称
    val stream = "word-counts-kinesis"
    //Kinesis 访问路径
    val endpoint = "https://kinesis.us-east-1.amazonaws.com"
    //一秒钟发送1000个Records
    val recordsPerSecond = "1000"
    //一个Record包含100个单词
    val wordsPerRecord = "10"

    val totals = generate(stream, endpoint, recordsPerSecond.toInt, wordsPerRecord.toInt)

    println("Totals for the words send")
    totals.foreach(println(_))
  }

  private def generate(stream: String,
                       endpoint: String,
                       recordsPerSecond: Int,
                       wordsPerRecord: Int): Seq[(String, Int)] = {
    //定义一个单词列表
    val randomWords = List("spark", "hadoop", "hive", "kinesis", "kinesis")
    val totals = scala.collection.mutable.Map[String, Int]()

    //建立Kinesis连接 这里aws_access_key_id,aws_secret_access_key已经存在本地credentials
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    kinesisClient.setEndpoint(endpoint)

    println(s"Putting records onto stream $stream and endpoint $endpoint at a rate of" +
      s" $recordsPerSecond records per second and $wordsPerRecord words per record")

    //根据recordsPerSecond 和 wordsPerRecord 将随机生成的单词放入Record
    for (i <- 1 to 2) {
      val records =(1 to recordsPerSecond.toInt).foreach {
        recordNum =>
          val data = (1 to wordsPerRecord.toInt).map(x => {
            val randomWordIdx = Random.nextInt(randomWords.size)
            val randomWord = randomWords(randomWordIdx)

            totals(randomWord) = totals.getOrElse(randomWord, 0) + 1

            randomWord
          }).mkString(" ")

          //创建一个分区键
          val partitionKey = s"partitionKey-$recordNum"
          //创建一个putRecordRequest
          val putRecordRequest = new PutRecordRequest().withStreamName(stream)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes))
          //将record放到stream中
          val putRecordResult = kinesisClient.putRecord(putRecordRequest)
      }

      Thread.sleep(1000)
      println("Sent " + recordsPerSecond + " records")
    }

    totals.toSeq.sortBy(_._1)
  }
}
