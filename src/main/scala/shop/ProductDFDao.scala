package shop

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * @author Evgeny Borisov
  */
class ProductDFDao(sparkSession:SparkSession) {
  import sparkSession.implicits._

  def getProductDF(): DataFrame  = {
    sparkSession.sparkContext.parallelize(Seq(Product(id=12,price = 100),Product(id=13,price = 200))).toDF()
  }
}
