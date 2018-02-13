/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.laaws.rs.io.index;

import org.lockss.laaws.rs.model.ArtifactIndexData;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Builder of artifact filtering predicates.
 */
public class ArtifactPredicateBuilder {
    // The individual filtering predicates.
    private Set<Predicate<ArtifactIndexData>> predicates = new HashSet<>();

    /**
     * Adds a filtering predicate by Archival Unit identifier.
     * 
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public ArtifactPredicateBuilder filterByAuid(String auid) {
        if (auid != null)
            predicates.add(x -> x.getAuid().equals(auid));
        return this;
    }

    /**
     * Adds a filtering predicate by indexing commit status.
     * 
     * @param committedStatus
     *          A Boolean with the commit status.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public ArtifactPredicateBuilder filterByCommitStatus(Boolean committedStatus) {
        if (committedStatus != null)
            predicates.add(artifact -> artifact.getCommitted() == committedStatus);
        return this;
    }

    /**
     * Adds a filtering predicate by repository collection.
     * 
     * @param collection
     *          A String with the collection identifier.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public ArtifactPredicateBuilder filterByCollection(String collection) {
        if (collection != null)
            predicates.add(artifact -> artifact.getCollection().equals(collection));
        return this;
    }

    /**
     * Adds a filtering predicate by URI prefix.
     * 
     * @param prefix
     *          A String with the URI prefix.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public ArtifactPredicateBuilder filterByURIPrefix(String prefix) {
        if (prefix != null)
            predicates.add(artifact -> artifact.getUri().startsWith(prefix));
        return this;
    }

    /**
     * Adds a filtering predicate by full URI.
     * 
     * @param uri
     *          A String with the URI.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public ArtifactPredicateBuilder filterByURIMatch(String uri) {
        if (uri != null)
            predicates.add(artifact -> artifact.getUri().equals(uri));
        return this;
    }

    /**
     * Adds a filtering predicate by version.
     * 
     * @param version
     *          A String with the version.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public ArtifactPredicateBuilder filterByVersion(String version) {
        if (version != null)
            predicates.add(artifact -> artifact.getVersion().equals(version));
        return this;
    }

    /**
     * Builds the full artifact filtering predicate.
     * 
     * @return a {@code Predicate<ArtifactIndexData>} with the full artifact
     *         filtering predicate.
     */
    public Predicate<ArtifactIndexData> build() {
        return predicates.stream().reduce(Predicate::and).orElse(include -> false);
    }
}
