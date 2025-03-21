import SbtDASPlugin.autoImport.*

lazy val root = (project in file("."))
  .enablePlugins(SbtDASPlugin)
  .settings(
    repoNameSetting := "das-excel",
    libraryDependencies ++= Seq(
      "com.raw-labs" %% "das-server-scala" % "0.6.0" % "compile->compile;test->test",
      // Apache POI for reading Excel files
      "org.apache.poi" % "poi" % "5.4.0",
      "org.apache.poi" % "poi-ooxml" % "5.4.0",
      // Scalatest
      "org.scalatest" %% "scalatest" % "3.2.19" % "test"),
    dependencyOverrides ++= Seq(
      "io.netty" % "netty-handler" % "4.1.118.Final"
    ))
