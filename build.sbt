

name := "AD"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.0" ,
  "org.apache.spark" %% "spark-mllib" % "1.6.0" ,
  "org.apache.spark" %% "spark-sql" % "1.6.0" ,
  "org.apache.spark" %% "spark-hive" % "1.6.0" ,
  "org.apache.spark" %% "spark-streaming" % "1.6.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "com.databricks" % "spark-csv_2.10" % "1.4.0"
)
libraryDependencies += "com.rockymadden.stringmetric" % "stringmetric-core_2.10" % "0.27.3"

mainClass in assembly := some("AD")
assemblyJarName := "author-disambiguation.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}