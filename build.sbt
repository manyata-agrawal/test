
name := """bodhee_jdbc_gateway_connector"""

version := "1.0.3"

lazy val root = (project in file(".")).enablePlugins(PlayJava,PlayEbean)

scalaVersion := "2.11.7"

playEbeanModels in Compile := Seq("models.*")

libraryDependencies ++= Seq(
  javaJdbc,
  cache,
  javaWs,
  evolutions,
  ws,
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8",
  "org.postgresql" % "postgresql" % "9.4-1201-jdbc41",
  "commons-io" % "commons-io" % "2.5",
  "commons-logging" % "commons-logging" % "1.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.javassist" % "javassist" % "3.20.0-GA",
 // "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0",
  "org.quartz-scheduler" % "quartz" % "2.2.3",
  "com.google.code.gson" % "gson" % "2.2.2",
  "net.sourceforge.jtds" % "jtds" % "1.3.0",
  "com.oracle.database.jdbc" % "ojdbc8" % "19.8.0.0"

)
libraryDependencies += filters
