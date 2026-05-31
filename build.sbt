import sbtrelease.ReleaseStateTransformations._

ThisBuild / organization         := "com.github.saadaouini"
ThisBuild / organizationName     := "saadaouini"
ThisBuild / organizationHomepage := Some(url("https://github.com/saadaouini"))
ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/saadaouini/spark-lens-listener"),
  "scm:git@github.com:saadaouini/spark-lens-listener.git",
))
ThisBuild / developers := List(Developer(
  id    = "saadaouini",
  name  = "saadaouini",
  email = "nidhal.saadaoui.1993@gmail.com",
  url   = url("https://github.com/saadaouini"),
))
ThisBuild / description  := "Zero-config Spark performance analyzer — attach via spark.extraListeners, get actionable recommendations in your logs."
ThisBuild / licenses     := List("Apache-2.0" -> new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage     := Some(url("https://github.com/saadaouini/spark-lens-listener"))

ThisBuild / scalaVersion       := "2.12.19"
ThisBuild / crossScalaVersions := Seq("2.12.19", "2.13.14")

// Publish settings (Maven Central via Sonatype)
ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / versionScheme := Some("semver-spec")

lazy val root = (project in file("."))
  .settings(
    name := "spark-lens",

    // Spark is provided by the cluster — do not bundle it
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.0" % "provided",
    ),

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
    ),

    // Make the jar as small as possible — only our code, Spark provided
    Compile / packageBin / mappings := (Compile / packageBin / mappings).value
      .filterNot { case (_, path) => path.startsWith("META-INF/maven") },

    scalacOptions ++= Seq(
      "-encoding", "utf8",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings",
    ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Ypartial-unification")
      case _             => Nil
    }),

    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
    ),

    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommandAndRemaining("+publishSigned"),
      releaseStepCommand("sonatypeBundleRelease"),
      setNextVersion,
      commitNextVersion,
      pushChanges,
    ),
  )
