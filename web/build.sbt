name := "wherehows"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "mysql" % "mysql-connector-java" % "5.1.22",
  "org.springframework" % "spring-context" % "4.1.1.RELEASE",
  "org.springframework" % "spring-jdbc" % "4.1.1.RELEASE",
  "org.mockito" % "mockito-core" % "1.9.5"
)

play.Project.playJavaSettings
