import AssemblyKeys._

name := "thunderain"

version := "0.1.0"

scalaVersion := "2.9.3"

retrieveManaged := true

assemblySettings

unmanagedJars in Compile <++= baseDirectory map { base =>
  val hiveFile = file(System.getenv("HIVE_HOME")) / "lib"
  val baseDirectories = (base / "lib") +++ (hiveFile)
  val customJars = (baseDirectories ** "*.jar")
  // Hive uses an old version of guava that doesn't have what we want.
  customJars.classpath.filter(!_.toString.contains("guava"))
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.8.0-incubating"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "0.8.0-incubating"

libraryDependencies += "edu.berkeley.cs.amplab" %% "shark" % "0.8.0-SNAPSHOT"

libraryDependencies += "org.tachyonproject" % "tachyon" % "0.3.0-SNAPSHOT"

resolvers ++= Seq(
   "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
   "Maven Repository" at "http://repo1.maven.org/maven2",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Spray Repository" at "http://repo.spray.cc/"
)
