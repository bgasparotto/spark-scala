name := "spark-scala"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
)

/* Adds provided dependencies to the main classpath when running the project from the IDE */
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated
