import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "scalding-ml"

version := "0.0.1"

scalaVersion := "2.10.3"

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases",
  "Concurrent Maven Repo" at "http://conjars.org/repo",
  "Codahale" at "http://repo.typesafe.com/typesafe/releases"
)

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "com.twitter" % "scalding-core_2.10" % "0.9.0rc4",
  "com.twitter" % "scalding-args_2.10" % "0.9.0rc4",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith("project.clj") => MergeStrategy.concat
    case s if s.endsWith(".html") => MergeStrategy.last
    case s if s.endsWith(".dtd") => MergeStrategy.last
    case s if s.endsWith(".xsd") => MergeStrategy.last
    case s if s.endsWith("pom.xml") => MergeStrategy.last
    case s if s.endsWith("pom.properties") => MergeStrategy.last
    case x => old(x)
  }
}