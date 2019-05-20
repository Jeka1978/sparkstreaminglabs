package vitaly

import java.util.Locale

import org.apache.spark.sql.streaming.{GroupState, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author Evgeny Borisov
  */
object MostPopularProductEver {
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
    val dataset = purchases.join(Products.getDf(spark).as("product"), $"purchase.product_id" === $"product.id")
      .select(
        $"product.id" as "id",
        $"product.name" as "name"
      )
    val products: Dataset[SimpleProduct] = dataset.as[SimpleProduct]

    val totalBuy = products.groupByKey(_.id).mapGroupsWithState((groupKey, iterator, state: GroupState[Long]) => {
      val name = iterator.next().name
      state.update(state.getOption.getOrElse(0L) + iterator.size + 1)
      //      x->state.get
      IdPerAmount(name, state.get)
    })


    val mostPopular = products
      .groupByKey(_ => 0)
      .mapGroupsWithState((x, productIterator, state: GroupState[Map[Long, Long]]) => {
        var stateMap: Map[Long, Long] = state.getOption.getOrElse(Map.empty)

        productIterator.foreach(product => {
          if (stateMap.contains(product.id)) stateMap = stateMap.updated(product.id, stateMap(product.id) + 1)
          else stateMap += (product.id -> 1)
        })
        state.update(stateMap)
        //            stateMap.maxBy(_._2)
        stateMap.toSeq.sortBy(_._2)(Ordering[Long].reverse).take(3)
      })
      .flatMap(identity)
      .select($"_1" as "id", $"_2" as "count")

    Console.write(mostPopular, mode = OutputMode.Update()).awaitTermination()

  }

  case class SimpleProduct(id: Long, name: String)

  case class IdPerAmount(name: String, amount: Long)


}


