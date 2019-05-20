package vitaly

import java.util.Locale
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object IncomeByAge {
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
      .join(Clients.getDf(spark).as("client"), $"purchase.client_id" === $"client.id")
      .join(Products.getDf(spark).as("product"), $"product_id" === $"product.id")
      .select(
        $"client.age" as "client_age",
        $"product.price" as "amount"
      )
      .withColumn("age_group",
        when($"client_age" <= 20, lit("under 20"))
          .when(col("client_age") <= lit(30), lit("20 - 30"))
          .when($"client_age" <= lit(40), lit("30 - 40"))
          .otherwise("> 50")
      )
      .groupBy($"age_group")
      .agg(avg($"amount"))
    Console.write(withMostPopularProduct, mode = OutputMode.Complete()).awaitTermination()  // without groupby
//    Console.write(withMostPopularProduct, mode = OutputMode.Complete()).awaitTermination()
  }


}