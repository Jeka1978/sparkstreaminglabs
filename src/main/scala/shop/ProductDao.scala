package shop

/**
  * @author Evgeny Borisov
  */
class ProductDao {
  def getById(id: Int): Product = {
    Product(id = id, price = 100)
  }
}
