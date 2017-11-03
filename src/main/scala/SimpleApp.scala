import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

class SimpleApp{

  def main(args: Array[String]) {
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

      graph.vertices.filter {case (id, (name, pos, sex)) => pos == "" && sex == "m"}.count()

  }

}
