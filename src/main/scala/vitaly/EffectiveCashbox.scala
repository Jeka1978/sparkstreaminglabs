package vitaly

import java.util.Locale

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.TimestampType

object EffectiveCashbox {
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

    val withMostEffectiveCachbox = purchases
      .join(Products.getDf(spark), $"product_id" === $"id")
      .withColumn("window", window($"timestamp" cast TimestampType, "30 seconds", "30 seconds"))
      .select(
        $"cashbox_id",
        $"price" as "amount",
        $"window"
      )
      .as[CachboxWithWindow]
      .groupByKey(_.window)
      .mapGroups { (window, group) =>
        val (cashbox_id, amount) = group
          .toStream
          .groupBy(_.cashbox_id)
          .mapValues(_.map(_.amount).sum)
          .max(orderByAmount)

        (window, cashbox_id, amount)
      }
      .select($"_1" as "window", $"_2" as "cashbox_id", $"_3" as "amount")

    Console.write(withMostEffectiveCachbox, OutputMode.Append()).awaitTermination()
  }

  private val orderByAmount = Ordering.by((_: (Long, Double))._2)

  case class Window(start: java.sql.Timestamp, end: java.sql.Timestamp)

  case class CachboxWithWindow(cashbox_id: Long,
                               amount: Double,
                               window: Window)

}