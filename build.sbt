name := "graphx"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

// https://mvnrepository.com/artifact/org.alluxio/alluxio-core-client-internal
libraryDependencies += "org.alluxio" % "alluxio-core-client-internal" % "1.0.1"


assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
