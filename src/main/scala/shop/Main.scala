package shop

import org.apache.spark._
import org.apache.spark.streaming._

/**
  * @author Evgeny Borisov
  */
object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    var productMap = (0 to 10).map(i => (i, Product(id = i, price = 100 + i))).toMap

    val productMapBroadcasted = ssc.sparkContext.broadcast(productMap)
    lines.map(Purchase.fromLine)
      .map(p => p.copy(price = productMapBroadcasted.value(p.product_id).price))
      .print()

 /*   lines.map(Purchase.fromLine)
      .transform(rdd =>
        rdd.mapPartitions(iterator => {
          val productDao = new ProductDao()
          iterator.map(purchase =>
            purchase.copy(price = productDao.getById(purchase.product_id).price))
        }))
      .print()*/

    // Print the first ten elements of each RDD generated in this DStream to the console
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}
