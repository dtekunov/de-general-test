plugins {
    scala
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.13.6")
    implementation("org.apache.spark:spark-sql_2.13:3.2.0") //return: compile-only
    implementation("com.amazonaws:aws-java-sdk:1.12.136")
    implementation("com.github.pureconfig:pureconfig_2.13:0.17.1")
    implementation("com.github.scopt:scopt_2.13:4.0.0")


    implementation("com.google.guava:guava:30.1.1-jre")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.scalatest:scalatest_2.13:3.2.9")
    testImplementation("org.scalatestplus:junit-4-13_2.13:3.2.2.0")

    testRuntimeOnly("org.scala-lang.modules:scala-xml_2.13:1.2.0")
}

application {
    // Define the main class for the application.
    mainClass.set("com.di.Main")
}
