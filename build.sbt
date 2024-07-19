val scala3Version = "3.4.0"

lazy val libs = Seq(
  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  ("org.apache.spark" %% "spark-sql-api" % "3.5.1").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-connect-client-jvm" % "3.5.1").cross(CrossVersion.for3Use2_13),
//  // https://mvnrepository.com/artifact/org.scala-lang/scala-compiler
//  "org.scala-lang" %% "scala3-compiler" % "3.4.0"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "spark-scalatest-gen",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version, 
    libraryDependencies ++= libs

  )
