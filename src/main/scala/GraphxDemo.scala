import src._
import com.mongodb.casbah.Imports._

import scala.util.parsing.json.JSON

object GraphxDemo{

  def main(args: Array[String]): Unit = {
    val mongoClient: MongoClient = new MongoData().getCollections("localhost", 27017)
    getRelations(mongoClient, "user_collection").foreach(println(_))
  }

  // get user vertex
  def getUser(mongoClient: MongoClient, collection: String): Array[(Long, String)] = {
    val userCollection = mongoClient("test")(collection)
    val filter_user = MongoDBObject(
      "id" -> 1,
      "stock_list" -> 1,
      "name" -> 1
    )

    userCollection.find(MongoDBObject.empty, filter_user).limit(1000).toArray
      .map(e => Tuple2(e.get("id").toString.toLong, e.get("name").toString))
  }

  // get stock vertex
  def getStock(mongoClient: MongoClient, collection: String): Array[(Long, String)] = {
      val stockCollection = mongoClient("test")(collection)
      val filter_stock = MongoDBObject(
        "_id" -> 0,
        "full_code" -> 1,
        "name" -> 1
      )

      stockCollection.find(MongoDBObject.empty, filter_stock).toArray
        .map(e => Tuple2(e.get("full_code").toString.substring(2).toLong, e.get("name").toString))
    }

  def getRelations(mongoClient: MongoClient, collection: String): Array[(Long, Long, String)] = {
    val userCollections = mongoClient("test")(collection)
    val filter_relation = MongoDBObject(
      "id" -> 1,
      "stock_list" -> 1
    )
    userCollections.find(MongoDBObject.empty, filter_relation).limit(1000).toArray
          .filter(e => e.get("stock_list").toString.equals("[]")).map(toRelation).flatten
  }

  def toRelation(element: DBObject): Array[(Long, Long, String)] = {
    val user: Long = element.get("id").toString.toLong
    val stock_list = Array (element.get("stock_list"))
    println(stock_list)
    stock_list.map(e => JSON.parseRaw(e.toString).getOrElse("reduced_code").toString.toLong)
      .map(e => (user, e, "subscribe"))
  }
}
