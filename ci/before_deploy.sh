#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    gpg --fast-import ci/codesigning.asc
    docker login -u $DOCKER_USERNAME -p $DOCKER_PASSPHRASE ;
fi