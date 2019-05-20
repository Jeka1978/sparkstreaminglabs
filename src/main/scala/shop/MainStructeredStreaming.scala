package shop

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Evgeny Borisov
  */
object MainStructeredStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val spark =SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)

/*    lines.map(Purchase.fromLine)
      .transform(rdd => rdd.mapPartitions(iterator => {
        val productDao = new ProductDao()
        iterator.map(purchase => purchase.copy(price = productDao.getById(purchase.product_id).price))
      }))
      .print()*/

    // Print the first ten elements of each RDD generated in this DStream to the console
    ssc.start() // Start the computation
    ssc.awaitTermination()  }
}
