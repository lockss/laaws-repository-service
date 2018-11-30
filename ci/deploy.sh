#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
	mvn -P travis-maven-central --settings ci/settings.xml ;
    mvn -P travis-docker-hub ;
fi