import AssemblyKeys._

name := "streaming-demo"

version := "0.0.1"

scalaVersion := "2.9.2"

retrieveManaged := true

assemblySettings

libraryDependencies += "org.spark-project" %% "spark-core" % "0.7.0-SNAPSHOT"

libraryDependencies += "org.spark-project" %% "spark-streaming" % "0.7.0-SNAPSHOT"

libraryDependencies += "it.unimi.dsi" % "fastutil" % "6.4.2"

libraryDependencies += "com.googlecode.javaewah" % "JavaEWAH" % "0.4.2"

libraryDependencies += "org.tachyonproject" % "tachyon" % "0.2.0"

resolvers ++= Seq(
   "Maven Repository" at "http://repo1.maven.org/maven2",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Spray Repository" at "http://repo.spray.cc/"
)

unmanagedBase <<= baseDirectory { base => base / "lib" }

