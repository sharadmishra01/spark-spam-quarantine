scalaVersion := "2.11.6"

name := "spark-spam-quarantine"
organization := "com.om.sierra"
version := "0.0.1-SNAPSHOT"
publishMavenStyle := true

val sparkVersion = "2.4.5"
lazy val scalacheck = "org.scalacheck" %% "scalacheck" % "1.13.4"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  scalacheck % Test,
  "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5",
  "org.scalaj" % "scalaj-http_2.11" % "2.4.2",
  "io.spray" %% "spray-json" % "1.3.5"

)
excludeDependencies += ExclusionRule("javax.ws.rs", "javax.ws.rs-api")
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1"
dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "spark-project", "spark", "unused", xs @ _x) => MergeStrategy.last
  case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister", xs @_*) => MergeStrategy.first
  case PathList("META-INF", _@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

resolvers ++= Seq (
  "Artima Maven Repository" at "https://repo.artima.com/releases",
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

publishTo := {
  val nexus = ""
  if (isSnapshot.value)
    Some("snapshots" at nexus + "om")
  else
    Some("releases"  at nexus + "om")
}
