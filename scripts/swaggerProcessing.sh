#!/bin/bash
#
# Copyright (c) 2018-2020 Board of Trustees of Leland Stanford Jr. University,
# all rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
# IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# Except as contained in this notice, the name of Stanford University shall not
# be used in advertising or otherwise to promote the sale, use or other dealings
# in this Software without prior written authorization from Stanford University.
#

# Fixes the code generated by Swagger Codegen.

# Edit StatusApiDelegate.java.
STATUS_API_DELEGATE=src/generated/java/org/lockss/laaws/rs/api/StatusApiDelegate.java
sed -i.backup "s/import org.lockss.laaws.rs.model.ApiStatus/import org.lockss.util.rest.status.ApiStatus/" $STATUS_API_DELEGATE && rm $STATUS_API_DELEGATE.backup

# Edit StatusApi.java.
STATUS_API=src/generated/java/org/lockss/laaws/rs/api/StatusApi.java
sed -i.backup "s/import org.lockss.laaws.rs.model.ApiStatus/import org.lockss.util.rest.status.ApiStatus/" $STATUS_API && rm $STATUS_API.backup

# Edit CollectionsApiDelegate.java.
COLLECTIONS_API_DELEGATE=src/generated/java/org/lockss/laaws/rs/api/CollectionsApiDelegate.java
sed -i.backup "s/import org.lockss.laaws.rs.model.StreamingResponseBody/import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody/" $COLLECTIONS_API_DELEGATE && rm $COLLECTIONS_API_DELEGATE.backup

# Edit CollectionsApi.java.
COLLECTIONS_API=src/generated/java/org/lockss/laaws/rs/api/CollectionsApi.java
sed -i.backup "s/import org.lockss.laaws.rs.model.StreamingResponseBody/import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody/" $COLLECTIONS_API && rm $COLLECTIONS_API.backup
