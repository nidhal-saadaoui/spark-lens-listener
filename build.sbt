// ── Shared metadata (published to Maven Central for all subprojects) ──────────
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
ThisBuild / dynverSonatypeSnapshots := true

val sparkVersion = "3.5.0"

// ── Shared compiler & test settings ──────────────────────────────────────────
val commonSettings = Seq(
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
  pgpPassphrase          := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray),
)

// ── core: model, analyzers, reporters — no Spark dependency ──────────────────
lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "spark-lens-core",
    libraryDependencies ++= Seq(
      // spark-core is provided for Hadoop FS used in Reporter.writeOrPrint
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "org.slf4j"        %  "slf4j-api"  % "1.7.36"     % "provided",
      "org.scalatest"    %% "scalatest"  % "3.2.18"      % Test,
    ),
  )

// ── listener: SparkLensListener + SparkAppModelBuilder ───────────────────────
// When bumping Spark version: validate PlanAnalyzer and JoinAnalyzer string
// matches against a real FORMATTED plan dump from the new version. Fragile
// checks: "Statistics(sizeInBytes=" and "\n\n(" tree/detail boundary.
lazy val listener = (project in file("listener"))
  .dependsOn(core, core % "test->test")
  .settings(commonSettings)
  .settings(
    name := "spark-lens",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "org.slf4j"        %  "slf4j-api"  % "1.7.36"     % "provided",
    ),
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test,

    // Assembly JAR — Spark stays provided so the JAR is small
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case x                                    => (assembly / assemblyMergeStrategy).value(x)
    },
    assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-assembly.jar",
  )

// ── testing: SparkLensSpec, SparkLensSuite, SparkLensMatchers ────────────────
lazy val testing = (project in file("testing"))
  .dependsOn(core, listener)
  .settings(commonSettings)
  .settings(
    name := "spark-lens-testing",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      // scalatest is a compile dependency (not Test) — users depend on it transitively
      "org.scalatest"    %% "scalatest"  % "3.2.18",
    ),
    // Spark 3.5 / Hadoop 3.3.4 requires JVM < 23 (Subject.getSubject was removed in Java 21+).
    // When the current JVM is >= 23, auto-detect a Java 17 installation and use it for tests.
    // Override by setting JAVA_17_HOME in your environment.
    Test / javaHome := {
      val currentMajor = sys.props("java.version").split("\\.")(0).toInt
      if (currentMajor < 23) None
      else Seq(
        sys.env.get("JAVA_17_HOME"),
        Some("/opt/homebrew/opt/openjdk@17"),
        Some("/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"),
        Some("/usr/lib/jvm/java-17-openjdk-amd64"),
        Some("/usr/lib/jvm/java-17-openjdk"),
      ).flatten.map(file).find(f => (f / "bin" / "java").exists)
    },
  )

// ── root: aggregator only, not published ─────────────────────────────────────
lazy val root = (project in file("."))
  .aggregate(core, listener, testing)
  .settings(
    publish / skip        := true,
    publishLocal / skip   := true,
    // Prevent sbt from looking for sources in the root src/ during aggregate builds
    Compile / unmanagedSourceDirectories := Nil,
    Test    / unmanagedSourceDirectories := Nil,
  )
