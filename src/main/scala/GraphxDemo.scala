import src._
import com.mongodb.casbah.Imports._

object GraphxDemo{

  def main(args: Array[String]): Unit = {



  }

  def getUser(): List[(Long, String)] = {
    val mongoHandle: MongoData = new MongoData()
    val userCollection = mongoHandle.getCollections("localhost", 27017, "test", "user")
    val filter_user = MongoDBObject(
      "id" -> 1,
      "stock_list" -> 1,
      "name" -> 1
    )

    mongoHandle.readData(userCollection, filter_user, 100)
      .map(e => Tuple2(e.get("id").toString.substring(2).toLong, e.get("name").toString))
  }

  def getStock(): List[(Long, String)] = {
    val mongoHandle: MongoData = new MongoData()
    val stockColllection = mongoHandle.getCollections("localhost", 27017, "test", "stock")
    val filter_stock = MongoDBObject(
      "_id" -> 0,
      "full_code" -> 1,
      "name" -> 1
    )
    mongoHandle.readData(stockColllection, filter_stock, -1).
      map(e => Tuple2(e.get("full_code").toString.substring(2).toLong, e.get("name").toString))
  }
}
