name := "graphx"

version := "1.0"

scalaVersion := "2.11.8"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

//libraryDependencies += "graphframes" % "graphframes" % "0.5.0-spark2.0-s_2.11"

// https://mvnrepository.com/artifact/org.alluxio/alluxio-core-client-internal
//libraryDependencies += "org.alluxio" % "alluxio-core-client-internal" % "1.0.1"


assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
