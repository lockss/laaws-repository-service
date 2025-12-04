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

dependencies {
    // Internal dependencies
    api(project(":lockss-spring-bundle"))

    // PostgreSQL
    api(libs.postgresql)

    // Test dependencies
    testImplementation(platform(project(":lockss-pom-bundles:lockss-junit5-bundle")))
    testImplementation(libs.junit.jupiter.engine)
    testImplementation(libs.embedded.postgres)
}

// Docker configuration
docker {
    imageName.set("laaws-repository-service")
    restPort.set(24610)
    uiPort.set(24611)
}
