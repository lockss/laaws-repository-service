/*
 * LAAWS Repository Service
 *
 * LOCKSS Repository Service providing REST API for content storage.
 */

plugins {
    id("lockss-spring-boot-conventions")
}

group = "org.lockss.laaws"
version = "2.16.0-SNAPSHOT"
description = "LOCKSS Repository Service"

// OpenAPI code generation configuration
openapi {
    specFile.set(file("src/main/resources/swagger/swagger.yaml"))
    basePackage.set("org.lockss.laaws.rs")
}

dependencies {
    // Internal dependencies
    api(project(":lockss-spring-bundle"))

    // PostgreSQL
    api(libs.postgresql)

    // SpringDoc OpenAPI (for generated code)
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:2.3.0")

    // Test dependencies
    testImplementation(platform(project(":lockss-pom-bundles:lockss-junit5-bundle")))
    testImplementation(libs.junit.jupiter.engine)
    testImplementation(libs.embedded.postgres)
    testImplementation(project(":lockss-spring-bundle", configuration = "testArtifacts"))
    testImplementation(project(":lockss-core", configuration = "testArtifacts"))
}

// Docker configuration
docker {
    imageName.set("laaws-repository-service")
    restPort.set(24610)
    uiPort.set(24611)
}
