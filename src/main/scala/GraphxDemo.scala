import src._
import com.mongodb.casbah.Imports._

object GraphxDemo{

  def main(args: Array[String]): Unit = {
    val mongoHandle: MongoData = new MongoData()
    val postCollection = mongoHandle.getCollections("localhost", 27017, "test", "user")
    val filter = MongoDBObject(
      "id" -> 1,
      "stock_list" -> 1
    )
    val data = mongoHandle.readData(postCollection, filter)
    val users = data.map(e => Tuple2(e.get("id"), ()))
    data.map(e => List (e.get("stock_list"))).foreach(println(_))
  }
}
