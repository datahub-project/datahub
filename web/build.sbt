name := "wherehows"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayJava, SbtTwirl)

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  javaJdbc,
  javaWs,
  cache,
  filters,
  "mysql" % "mysql-connector-java" % "5.1.40",
  "org.springframework" % "spring-jdbc" % "4.1.6.RELEASE",
  "org.mockito" % "mockito-core" % "1.10.19"
)
