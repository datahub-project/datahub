name := "backend-service"

version := "1.0-SNAPSHOT"

unmanagedJars in Compile <++= baseDirectory map { base =>
  val dirs = (base / "metadata-etl/extralibs") +++ (base / "extralibs")
  (dirs ** "*.jar").classpath
}

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "mysql" % "mysql-connector-java" % "5.1.22",
  "org.springframework" % "spring-context" % "4.1.1.RELEASE",
  "org.springframework" % "spring-jdbc" % "4.1.1.RELEASE",
  "org.mockito" % "mockito-core" % "1.9.5",
  "org.quartz-scheduler" % "quartz" % "2.2.1",
  "org.quartz-scheduler" % "quartz-jobs" % "2.2.1",
  "org.slf4j" % "slf4j-api" % "1.6.6",
  "org.jasypt" % "jasypt" % "1.9.2",
  "org.apache.kafka" % "kafka_2.10" % "0.10.0.0",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
)

play.Project.playJavaSettings
