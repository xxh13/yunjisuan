import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

object Hello extends App {
  val sc: SparkContext = new SparkContext("local[2]", "demo")
  //      val users = sc.textFile("graph/user.txt").
  //        map(line => line.split(",")).map(parts => (parts.head.toLong, parts.tail))
  val users : RDD[(Long, (String, String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student", "m")), (7L, ("jgonzal", "postdoc", "m")),
    (5L, ("franklin", "prof", "f")), (2L, ("istoica", "prof", "f"))))

  val relationships : RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

  val defaultUser = ("John Doe", "missing", "m")

  val graph = Graph(users, relationships, defaultUser)

//  graph.edges.saveAsTextFile("file:///user")

//  graph.vertices.saveAsTextFile("file:///relationship")

  val result = graph.vertices.filter {case (id, (name, pos, sex)) => sex == "m"}.count()

//  println(result)
  val facts: RDD[String] =
    graph.triplets.map(triplet => triplet.srcAttr._1 + " job: " + triplet.srcAttr._2 + " gender:" + triplet.srcAttr._3
      + " is the " + triplet.attr + " of " + triplet.dstAttr._1)

  facts.collect.foreach(println(_))


}
