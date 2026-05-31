ThisBuild / organization         := "io.github.nidhal-saadaoui"
ThisBuild / organizationName     := "saadaouini"
ThisBuild / organizationHomepage := Some(url("https://github.com/nidhal-saadaoui"))
ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/nidhal-saadaoui/spark-lens-listener"),
  "scm:git:git://github.com/nidhal-saadaoui/spark-lens-listener.git",
  Some("scm:git:ssh://github.com/nidhal-saadaoui/spark-lens-listener.git"),
))
ThisBuild / developers := List(Developer(
  id    = "saadaouini",
  name  = "Nidhal Saadaoui",
  email = "nidhal.saadaoui.1993@gmail.com",
  url   = url("https://github.com/nidhal-saadaoui"),
))
ThisBuild / description := "Zero-config Spark performance analyzer — attach via spark.extraListeners, get actionable recommendations in your logs."
ThisBuild / licenses    := List("Apache-2.0" -> new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage    := Some(url("https://github.com/nidhal-saadaoui/spark-lens-listener"))

ThisBuild / scalaVersion          := "2.12.20"
ThisBuild / crossScalaVersions    := Seq("2.12.20", "2.13.15")
ThisBuild / dynverSonatypeSnapshots := true  // appends -SNAPSHOT for non-tagged commits

lazy val root = (project in file("."))
  .settings(
    name := "spark-lens",

    // Spark is provided by the cluster — do not bundle it
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.0" % "provided",
    ),

    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test,

    // Assembly JAR — Spark stays provided so the JAR is small
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case x                                    => (assembly / assemblyMergeStrategy).value(x)
    },
    assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-assembly.jar",

    scalacOptions ++= Seq(
      "-encoding", "utf8",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings",
    ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Ypartial-unification", "-target:jvm-1.8")
      case Some((2, 13)) => Seq("-release", "8")
      case _             => Nil
    }),

    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    ),

    publishMavenStyle      := true,
    publishTo              := sonatypePublishToBundle.value,
    sonatypeCredentialHost := "central.sonatype.com",
    usePgpKeyHex("5D4E47CFFB9C4CE1"),

    pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray),
  )
