plugins {
    id("java")
    id("application")
}

group = "org.example"
version = "1.0-SNAPSHOT"

application {
    mainClass.set("org.example.Main")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    implementation("org.slf4j:slf4j-simple:2.0.9")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
}
