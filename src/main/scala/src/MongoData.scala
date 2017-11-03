package src

import com.mongodb.casbah.Imports._

class MongoData {

  def getCollections(uri: String, port: Int, db: String, collections: String) : MongoClient =  {
    MongoClient(uri, port)
  }

  def readData(collection: MongoCollection, filter: MongoDBObject, limit: Int) : List[DBObject] = {

      if(limit == -1) {
        collection.find(MongoDBObject.empty,filter).toList
      } else {
        collection.find(MongoDBObject.empty, filter).limit(limit).toList
      }

  }
}
