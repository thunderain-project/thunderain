import AssemblyKeys._

name := "streaming-demo"

version := "0.0.1"

scalaVersion := "2.9.3"

retrieveManaged := true

assemblySettings

unmanagedJars in Compile <++= baseDirectory map { base =>
  val hiveFile = file("/home/jerryshao/source-code/hive/build/dist") / "lib"
  val baseDirectories = (base / "lib") +++ (hiveFile)
  val customJars = (baseDirectories ** "*.jar")
  // Hive uses an old version of guava that doesn't have what we want.
  customJars.classpath.filter(!_.toString.contains("guava"))
}

libraryDependencies += "org.spark-project" %% "spark-core" % "0.8.0-SNAPSHOT"

libraryDependencies += "org.spark-project" %% "spark-streaming" % "0.8.0-SNAPSHOT"

libraryDependencies += "edu.berkeley.cs.amplab" %% "shark" % "0.7.0-SNAPSHOT"

resolvers ++= Seq(
   "Maven Repository" at "http://repo1.maven.org/maven2",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Spray Repository" at "http://repo.spray.cc/"
)
