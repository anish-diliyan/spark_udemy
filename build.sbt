name := "spark-udemy"

version := "0.1"

scalaVersion := "2.13.14"

val sparkVersion = "3.5.1"
val vegasVersion = "0.3.11"
val postgresVersion = "42.7.3"
val sqliteVersion = "3.46.0.0"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
  // sqlite for Db connectivity
  "org.xerial" % "sqlite-jdbc" % sqliteVersion
)