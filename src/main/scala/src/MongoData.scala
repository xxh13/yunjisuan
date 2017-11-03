package src

import com.mongodb.casbah.Imports._

class MongoData {

  def getCollections(uri: String, port: Int, db: String, collections: String) : MongoCollection =  {
    val mongoClient : MongoClient = MongoClient(uri, port)

    val database = mongoClient(db)

    val post_collection = database(collections)

    post_collection
  }

  def readData(collection: MongoCollection, filter: MongoDBObject) : List[DBObject] = {

      collection.find(MongoDBObject.empty,filter).limit(10).toList

  }
}
