# Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

FROM lockss/lockss-alpine:3.9-2

MAINTAINER "LOCKSS Buildmaster" <buildmaster@lockss.org>

# Mandatory build arguments
ARG LOCKSS_MAVEN_GROUP
ARG LOCKSS_MAVEN_ARTIFACT
ARG LOCKSS_MAVEN_VERSION
ARG LOCKSS_REST_PORT
RUN test -n "${LOCKSS_MAVEN_GROUP}"    || exit 1 \
 && test -n "${LOCKSS_MAVEN_ARTIFACT}" || exit 2 \
 && test -n "${LOCKSS_MAVEN_VERSION}"  || exit 3 \
 && test -n "${LOCKSS_REST_PORT}"      || exit 4

# Optional build arguments
ARG LOCKSS_UI_PORT

# Environment variables
ENV LOCKSS_MAVEN_GROUP="${LOCKSS_MAVEN_GROUP}" \
    LOCKSS_MAVEN_ARTIFACT="${LOCKSS_MAVEN_ARTIFACT}" \
    LOCKSS_MAVEN_VERSION="${LOCKSS_MAVEN_VERSION}" \
    LOCKSS_REST_PORT="${LOCKSS_REST_PORT}" \
    LOCKSS_UI_PORT="${LOCKSS_UI_PORT}" \
    CONFIGS=/run/configs \
    SECRETS=/run/secrets \
    LOCKSS_HOME=/usr/local/share/lockss \
    LOCKSS_PIDS=/var/run \
    LOCKSS_DATA=/data \
    LOCKSS_TMP=/data/temp \
    LOCKSS_LOGS=/var/log/lockss

EXPOSE ${LOCKSS_REST_PORT} ${LOCKSS_UI_PORT}

RUN ipm-update \
 && ipm-install gettext \
                netcat \
                openjdk8-jre \
 && ipm-clean

ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]

HEALTHCHECK --retries=1 --start-period=60s CMD ["/usr/local/bin/docker-healthcheck"]

COPY /docker/bin/* /usr/local/bin/
COPY /target/current-with-deps.jar "${LOCKSS_HOME}/lib/lockss.jar"
