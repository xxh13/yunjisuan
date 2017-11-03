name := "graphx"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion,
                            "org.apache.spark" %% "spark-graphx" % sparkVersion,
                            "org.mongodb" %% "casbah" % "3.1.1"
)