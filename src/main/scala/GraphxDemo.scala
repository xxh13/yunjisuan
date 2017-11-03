import src._
import com.mongodb.casbah.Imports._

object GraphxDemo{

  def main(args: Array[String]): Unit = {



  }

  // get user vertex
  def getUser(mongoClient: MongoClient, collection: String): List[(Long, String)] = {
    val userCollection = mongoClient("test")(collection)
    val filter_user = MongoDBObject(
      "id" -> 1,
      "stock_list" -> 1,
      "name" -> 1
    )

    userCollection.find(MongoDBObject.empty, filter_user).limit(1000).toList
      .map(e => Tuple2(e.get("id").toString.toLong, e.get("name").toString))
  }

  // get stock vertex
  def getStock(mongoClient: MongoClient, collection: String): List[(Long, String)] = {
      val stockCollection = mongoClient("test")(collection)
      val filter_stock = MongoDBObject(
        "_id" -> 0,
        "full_code" -> 1,
        "name" -> 1
      )

      stockCollection.find(MongoDBObject.empty, filter_stock).toList
        .map(e => Tuple2(e.get("full_code").toString.substring(2).toLong, e.get("name").toString))
    }

}
