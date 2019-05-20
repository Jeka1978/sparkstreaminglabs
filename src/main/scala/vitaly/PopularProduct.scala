package vitaly

import java.util.Locale

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object PopularProduct {
  def main(args: Array[String]): Unit = {
    Locale.setDefault(Locale.UK)

    val spark = SparkSession.builder()
      .appName("PopularProduct")
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

    val purchases = WebSocket.getDf(spark).as("purchase")

    val withMostPopularProduct = purchases
      .join(Products.getDf(spark).as("product"), $"purchase.product_id" === $"product.id")
      .select(
        $"purchase.timestamp" as "timestamp",
        $"product.id" as "product_id",
        $"product.name" as "product_name"
      )
      .withColumn("window", window($"timestamp" cast TimestampType, "30 seconds", "30 seconds"))
      .as[PurchaseWithProduct]
      .groupByKey(_.window)
      .mapGroups {  (groupKey, group) =>
        val mostPopular: (String, Int) = group
          .toStream  //like list, but lazy
          .groupBy(_.product_name)
          .mapValues(_.size)
          .max(Ordering.by((_: (String, Int))._2))

        mostPopular
      }
      .select($"_1" as "product_name", $"_2" as "count")

    Console.write(withMostPopularProduct).awaitTermination()
  }

  case class Window(start: java.sql.Timestamp, end: java.sql.Timestamp)

  case class PurchaseWithProduct(product_id: Long,
                                 timestamp: Long,
                                 product_name: String,
                                 window: Window)

}