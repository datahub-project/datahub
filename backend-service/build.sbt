name := "backend-service"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayJava)

scalaVersion := "2.10.6"

unmanagedJars in Compile <++= baseDirectory map { base =>
  val dirs = base / "extralibs"
  (dirs ** "*.jar").classpath
}

libraryDependencies ++= Seq(
  javaJdbc,
  cache,
  "mysql" % "mysql-connector-java" % "5.1.40",
  "org.mockito" % "mockito-core" % "1.10.19",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.jasypt" % "jasypt" % "1.9.2",
  "org.apache.kafka" % "kafka_2.10" % "0.10.0.1",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.1"
).map(_.exclude("log4j", "log4j"))
  .map(_.exclude("org.slf4j", "slf4j-log4j12"))
