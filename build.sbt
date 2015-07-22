import AssemblyKeys._

assemblySettings

name := "tryScala"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies +=  "org.apache.spark" % "spark-graphx_2.10" % "1.4.0" % "provided"

libraryDependencies +=  "joda-time" % "joda-time" % "2.8.1" % "provided"

libraryDependencies +=  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"