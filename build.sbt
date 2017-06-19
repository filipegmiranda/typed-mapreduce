name := "typed-mapreduce"

version := "1.0"

scalaVersion := "2.12.2"

logBuffered in Test := false

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "com.typesafe.akka" %% "akka-actor" % "2.5.2",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.2" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
        