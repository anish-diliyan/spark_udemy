name := "spark-udemy"

version := "0.1"

scalaVersion := "2.13.14"

val sparkVersion = "3.5.1"
val postgresVersion = "42.7.3"
val logVersion = "2.23.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % logVersion,
  "org.apache.logging.log4j" % "log4j-core" % logVersion,
  // pgsql for Db connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)