package org.lockss.laaws.rs.configuration;

import org.lockss.log.L4JLogger;
import org.lockss.rs.io.storage.ArtifactDataStoreVersion;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Spring component which parses the Spring application.properties for
 * LOCKSS Repository Service parameters:
 */
@Component
public class RepositoryServiceProperties {
  private final static L4JLogger log = L4JLogger.getLogger();
  private static final String ARG_START_REINDEX = "--start-reindex";

  @Autowired
  ApplicationArguments appArgs;

  // LOCKSS repository properties
  @Value("${repo.spec:volatile}") String repoSpec;
  @Value("${repo.state.dir:/data/state}") String repoStateDir;

  // Artifact data store properties
  @Value("${repo.datastore.spec:#{null}}") String datastoreSpec;

  @Value("${repo.datastore.local.basedirs:#{null}}") String localBaseDirs;
  @Value("${repo.datastore.local.basedir:#{null}}") String localbaseDir;

  @Value("${repo.datastore.hdfs.server:#{null}}") String hdfsEndpoint;
  @Value("${repo.datastore.hdfs.basedir:#{null}}") String hdfsBaseDir;

  // Artifact index properties
  @Value("${repo.index.spec:#{null}}") String indexSpec;
  @Value("${repo.index.dispatching.enable:#{false}}") boolean isDispatchingIndexEnabled;

  @Value("${repo.persistIndexName:#{null}}") String repoPersistIndexName;
  @Value("${repo.index.local.persistIndexName:#{null}}") String localPersistIndexName;

  @Value("${repo.index.solr.solrUrl:#{null}}") String solrEndpoint;
  @Value("${repo.index.solr.solrCollection:#{null}}") String solrCollectionName;
  @Value("${repo.index.solr.hardCommitInterval:15000}") long solrHardCommitInterval;

  private boolean indexNeedsReindex;
  private ArtifactDataStoreVersion dataStoreVersion;

  public String getRepositorySpec() {
    return repoSpec;
  }

  // Parse repo spec for repo type
  public String getRepositoryType() {
    return getRepoSpecParts()[0].trim().toLowerCase();
  }

  public String[] getRepoSpecParts() {
    if (StringUtil.isNullString(repoSpec)) {
      log.error("Missing repository configuration");
      throw new IllegalStateException("Repository not configured");
    }

    return repoSpec.split(":", 2);
  }

  public boolean isSolrArtifactIndex() {
    return getIndexSpec().equals("solr");
  }

  public boolean isSqlArtifactIndex() {
    return getIndexSpec().equals("derby") ||
           getIndexSpec().equals("pgsql");
  }

  public boolean isDispatchingIndexEnabled() {
    return isDispatchingIndexEnabled;
  }

  /**
   * Returns the index specification for the type of LOCKSS repository configured for use
   * by this LOCKSS Repository Service.
   */
  private String getIndexSpecForRepository() {
    switch (getRepositoryType()) {
      case "volatile":
        // Allow a volatile index to be created so that WARC compression can be configured
        // in the volatile artifact data store
        return "volatile";

      case "local":
        // Support for legacy repo.spec=local:X;Y;Z
        return "local";

      case "custom":
        return indexSpec;

      default:
        throw new IllegalArgumentException("Repository spec not supported: " + getRepositorySpec());
    }
  }

  public String getIndexSpec() {
    indexSpec = getIndexSpecForRepository();

    if (StringUtil.isNullString(indexSpec)) {
      log.error("Missing artifact index configuration");
      throw new IllegalStateException("Artifact index not configured");
    }

    return indexSpec.trim().toLowerCase();
  }

  public String getLocalPersistIndexName() {
    if (localPersistIndexName == null) {
      // Fallback to previous property key
      localPersistIndexName = repoPersistIndexName;

      if (localPersistIndexName == null) {
        log.error("No local persist index name specified");
        throw new IllegalArgumentException("No local persist index name");
      }
    }

    return localPersistIndexName;
  }

  public File[] getLocalBaseDirs() {
    if (localBaseDirs == null) {
      // Fallback to previous property key
      localBaseDirs = localbaseDir;

      if (localBaseDirs == null) {
        log.error("No local base directories specified");
        throw new IllegalArgumentException("No local base dirs");
      }
    }

    // Convert String paths to File array and return
    return Arrays.stream(localBaseDirs.split(";"))
        .map(File::new)
        .toArray(File[]::new);
  }

  public String getSolrEndpoint() {
    if (StringUtil.isNullString(solrEndpoint)) {
      log.error("Missing Solr base URL endpoint");
      throw new IllegalArgumentException("Missing Solr base URL endpoint");
    }

    return solrEndpoint;
  }

  public String getSolrCollectionName() {
    return solrCollectionName;
  }

  public long getSolrHardCommitInterval() {
    return solrHardCommitInterval;
  }

  public String getDatastoreSpec() {
    return datastoreSpec.trim().toLowerCase();
  }

  public String getHdfsEndpoint() {
    return hdfsEndpoint;
  }

  public String getHdfsBaseDir() {
    return hdfsBaseDir;
  }

  public File getRepositoryStateDir() {
    return new File(repoStateDir);
  }

  public boolean shouldStartOrResumeReindex() {
    if (repoStateDir == null) {
      throw new IllegalStateException("Repository state directory not configured");
    }

    // Path to reindex state file
    Path reindexStatePath = getRepositoryStateDir().toPath()
        .resolve(WarcArtifactDataStore.REINDEXING_STATE_FILE);

    File reindexingStateFile = reindexStatePath.toFile();

    return indexNeedsReindex || reindexingStateFile.exists() || appArgs.containsOption(ARG_START_REINDEX);
  }

  public void setIndexNeedsReindex(boolean indexNeedsReindex) {
    this.indexNeedsReindex = indexNeedsReindex;
  }

  public void setDataStoreVersionFromDisk(ArtifactDataStoreVersion dataStoreVersion) {
    this.dataStoreVersion = dataStoreVersion;
  }

  public ArtifactDataStoreVersion getDataStoreVersionFromDisk() {
    return dataStoreVersion;
  }
}
