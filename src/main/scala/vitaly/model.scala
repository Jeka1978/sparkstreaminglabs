package vitaly

case class Purchase(
                     product_id: Long,
                     client_id: Long,
                     cashbox_id: Long,
                     timestamp: Long
                   )

case class Product(
                    id: Long,
                    name: String,
                    price: Double
                  )

case class Client(
                   id: Long,
                   name: String,
                   gender: String,
                   age: Int
                 )