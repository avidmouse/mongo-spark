name := "mongo-spark"

version := "1.0"

scalaVersion := "2.10.4"

lazy val core = project.in(file("core"))

lazy val streaming = project.in(file("streaming")).dependsOn(core)

lazy val examples = project.in(file("examples")).dependsOn(core, streaming)