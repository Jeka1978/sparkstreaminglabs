package vitaly

import org.apache.spark.sql.{Dataset, Encoders, ForeachWriter, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import scala.util.Random

object WebSocket {
  def getDf(spark: SparkSession): Dataset[Purchase] = {
    import spark.implicits._
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .withColumn("purchase_json", from_json($"value", Encoders.product[Purchase].schema))
      .select($"purchase_json.*")
      .as[Purchase].as("purchase")
  }
}

object Products {
  def getDf(spark: SparkSession): Dataset[Product] = {
    import spark.implicits._

    spark.sparkContext.parallelize((1 to 10).map(id => Product(id, randomName, id * 10 + 0.99))).toDS()
  }

  private def randomName = {
    val names = List("Coca cola", "Pepsi", "Lays", "iPhone XR")

    Random.shuffle(names).head
  }
}

object Clients {
  def getDf(spark: SparkSession): Dataset[Client] = {
    import spark.implicits._

    spark.sparkContext.parallelize((1 to 10).map(id => Client(id, randomName, randomGender, age = Random.nextInt(60)))).toDS()
  }

  private def randomGender = Random.shuffle(List("Male", "Female")).head

  private def randomName = {
    val names = List("John", "Will", "Cate", "Cassandra")

    Random.shuffle(names).head
  }
}

object Console {

  def print(ds: Dataset[_],mode: OutputMode = OutputMode.Append()): StreamingQuery = {
    class Writer[A] extends ForeachWriter[A] {
      def open(partitionId: Long, epochId: Long): Boolean = true
      def process(value: A): Unit = println(value)
      def close(errorOrNull: Throwable): Unit = ()
    }

    ds.writeStream.outputMode(mode).foreach(new Writer).start()

  }

  def write(ds: Dataset[_], mode: OutputMode = OutputMode.Append()): StreamingQuery =
    ds
      .writeStream
      .format("console")
      .outputMode(mode)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
}