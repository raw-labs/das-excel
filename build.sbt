import SbtDASPlugin.autoImport.*

lazy val root = (project in file("."))
  .enablePlugins(SbtDASPlugin)
  .settings(
    repoNameSetting := "das-salesforce",
    libraryDependencies ++= Seq(
      "com.raw-labs" %% "das-server-scala" % "0.4.0" % "compile->compile;test->test",
      "com.raw-labs" %% "protocol-das" % "1.0.0" % "compile->compile;test->test",
      // Apache POI for reading Excel files
      "org.apache.poi" % "poi" % "5.4.0",
      "org.apache.poi" % "poi-ooxml" % "5.4.0",
      // Scalatest
      "org.scalatest" %% "scalatest" % "3.2.19" % "test"))
