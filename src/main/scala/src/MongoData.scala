package src

import com.mongodb.casbah.Imports._

class MongoData {

  def getCollections(uri: String, port: Int) : MongoClient =  {
    MongoClient(uri, port)
  }
}
