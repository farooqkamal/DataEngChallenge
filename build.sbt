
name := "DataEngChallenge"

organization := "com.farooqkamal."

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-language:higherKinds",
  "-language:reflectiveCalls",
  "-Ywarn-dead-code",
  "-unchecked",
  "-Xfatal-warnings"
)

publishArtifact in Test := true
fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" %% "spark-catalyst" % "2.2.1",
  "org.apache.spark" %% "spark-hive" % "2.2.1",
  "ch.hsr" % "geohash" % "1.3.0",

  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
  "org.pegdown" % "pegdown" % "1.6.0" % Test

)
