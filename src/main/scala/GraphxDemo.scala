import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import com.mongodb.{BasicDBList, BasicDBObject}
import src.MongoData
import com.mongodb.casbah.Imports._


object GraphxDemo{

  def main(args: Array[String]): Unit = {
    val mongoClient: MongoClient = new MongoData().getCollections("localhost", 27017)
    val raw = getUser(mongoClient, "user")
    val users = raw._1
    val relations = raw._2
    val stock = getStock(mongoClient, "stock")

    val vertexes = users ++ stock

    val graph = getGraphx(vertexes, relations)

    val facts: RDD[String] =
      graph.triplets.map(triplet => triplet.srcAttr + " " +triplet.attr + " " + triplet.dstAttr)
    facts.collect.foreach(println(_))
  }

  def getGraphx(vertex: Array[(VertexId, String)], edge: Array[Edge[(String)]]): Graph[String, String] = {
    val sc = new SparkContext("local[2]", "graphx")
    val users: RDD[(VertexId, String)] = sc.parallelize(vertex)
    var relations: RDD[Edge[String]] = sc.parallelize(edge)
    val defaultUser = "default"

    Graph(users, relations, defaultUser)
  }

  // get user vertex
  def getUser(mongoClient: MongoClient, collection: String): (Array[(Long, String)], Array[Edge[String]]) = {
    val userCollection = mongoClient("test")(collection)
    val filter_user = MongoDBObject(
      "id" -> 1,
      "stock_list" -> 1,
      "name" -> 1
    )

    val rawData = userCollection.find(MongoDBObject.empty, filter_user).limit(1000).toArray
    val users = rawData.map(e => (e.get("id").toString.toLong, e.get("name").toString))
    val relations = rawData.map(e => Tuple2(e.get("id").toString.toLong, e.as[BasicDBList]("stock_list")))
      .filter(e => e._2.length != 0)
      .flatMap(toRelation)
    (users, relations)
  }

  // get stock vertex
  def getStock(mongoClient: MongoClient, collection: String): Array[(Long, (String))] = {
      val stockCollection = mongoClient("test")(collection)
      val filter_stock = MongoDBObject(
        "_id" -> 0,
        "full_code" -> 1,
        "name" -> 1
      )

      stockCollection.find(MongoDBObject.empty, filter_stock).toArray
          .filter(e => !isNumber(e.get("full_code").toString))
        .map(e => Tuple2(e.get("full_code").toString.substring(2).toLong, e.get("name").toString))
  }

  def toRelation(tuple2: Tuple2[Long, BasicDBList]): Array[Edge[String]] = {
    tuple2._2
      .filter(e => isNumber(e.asInstanceOf[BasicDBObject].getString("reduced_code")))
        .map(e => Edge(tuple2._1, e.asInstanceOf[BasicDBObject].get("reduced_code").toString.toLong, "subscribe")).toArray
  }

  def isNumber(id: String): Boolean = {
     try {
        id.toLong
        true
     } catch {
       case e: Exception => false
     }
  }
}
