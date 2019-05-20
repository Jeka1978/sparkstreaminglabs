package vitaly

import java.util.Locale
import org.apache.spark.sql._
import functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.TimestampType

object MoneyIncome {
  def main(args: Array[String]): Unit = {
    Locale.setDefault(Locale.UK)

    val spark = SparkSession.builder()
      .appName("EffectiveCashbox")
      .master("local[*]")
      .getOrCreate()

    try {
      impl(spark)
    } finally {
      spark.stop()
    }
  }

  private def impl(spark: SparkSession): Unit = {
    import spark.implicits._

    val purchases = WebSocket.getDf(spark)

    val totalIncome = purchases
      .join(Products.getDf(spark), $"product_id" === $"id")
      .groupBy(window($"timestamp" cast TimestampType, "30 seconds", "30 seconds"))
      .agg(sum($"price"))

//    totalIncome.writeStream.format("csv").outputMode(OutputMode.Complete()).start("C:\\Hadoop\\output")

    Console.print(totalIncome).awaitTermination()
  }


}