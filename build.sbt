name := "spark-catalyst-hsm-example"
version := "0.1"
organization := "br.com.wiklich"

scalaVersion := "2.12.15"

crossPaths := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"
)

enablePlugins(AssemblyPlugin)
assembly / assemblyJarName := "spark-catalyst-crypto.jar"
assembly / test := {}
assembly / mainClass := None
import sbtassembly.MergeStrategy

assembly / assemblyMergeStrategy := {
  case PathList("module-info.class") =>
    MergeStrategy.discard

  case PathList("META-INF", "MANIFEST.MF") =>
    MergeStrategy.discard

  case PathList("META-INF", "INDEX.LIST") =>
    MergeStrategy.discard

  case PathList("META-INF", name)
      if name.toLowerCase.endsWith(".sf") ||
        name.toLowerCase.endsWith(".dsa") ||
        name.toLowerCase.endsWith(".rsa") =>
    MergeStrategy.discard

  case PathList("META-INF", _ @_*) =>
    MergeStrategy.first

  case PathList("reference.conf") =>
    MergeStrategy.concat

  case PathList("application.conf") =>
    MergeStrategy.concat

  case PathList(ps @ _*) if ps.last endsWith ".properties" =>
    MergeStrategy.first

  case _ =>
    MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadedgoogle.@1").inAll
)

retrieveManaged := true
conflictWarning := ConflictWarning.disable
