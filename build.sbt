import sbt.*
import sbt.Keys.*
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.15"

enablePlugins(AssemblyPlugin)

lazy val root = (project in file("."))
  .settings(
    name := "CS-441-HW",

    libraryDependencies ++= Seq(
      // Hadoop dependencies
      "org.apache.hadoop" % "hadoop-common" % "3.3.6",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.6",

      "org.apache.commons" % "commons-text" % "1.12.0",
      "org.apache.commons" % "commons-math3" % "3.6.1",

      // JFastText dependency for word embeddings
      "com.github.vinhkhuc" % "jfasttext" % "0.5",

      // Tokenization dependencies
      "com.knuddels" % "jtokkit" % "1.1.0",

      // logging dependencies
      "org.slf4j" % "slf4j-api" % "2.0.16",
      "org.slf4j" % "slf4j-simple" % "2.0.16",
      "ch.qos.logback" % "logback-classic" % "1.5.6", // Logback backend

      // Configuration management
      "com.typesafe" % "config" % "1.4.3",

      "org.bytedeco" % "javacpp" % "1.5.10",

      // Scala tests
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.mockito" %% "mockito-scala" % "1.17.37" % Test,
    ),

    resolvers ++= Seq(
      "Maven Central" at "https://repo1.maven.org/maven2/",
      "jitpack.io" at "https://jitpack.io"
    ),

    assembly / assemblyJarName := "CS-441-HW-assembly-0.1.jar",

    assembly / mainClass := Some("app.MainApp"),

    // sbt-assembly settings for creating a fat jar
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs match {
          case "MANIFEST.MF" :: Nil => MergeStrategy.discard
          case "services" :: _ => MergeStrategy.concat
          case _ => MergeStrategy.discard
        }
      case "reference.conf" => MergeStrategy.concat
      case x if x.endsWith(".proto") => MergeStrategy.rename
      case _ => MergeStrategy.first
    }
  )
