package org.lockss.laaws.rs.controller;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.core.LocalLockssRepository;
import org.lockss.laaws.rs.core.LockssNoSuchArtifactIdException;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.io.storage.local.LocalWarcArtifactDataStore;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactSpec;
import org.lockss.log.L4JLogger;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.test.LockssTestCase4;
import org.lockss.util.ListUtil;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class TestLockssRepositoryStressTest extends SpringLockssTestCase4 {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Random TCP port assigned to the embedded servlet by the Spring Boot test environment.
   */
  @LocalServerPort
  private int port;

  static List<File> tmpDirs = new ArrayList<>();;

  // *******************************************************************************************************************
  // * SPRING TEST CONFIGURATION
  // *******************************************************************************************************************

  @TestConfiguration
  @Profile("test")
  static class TestLockssRepositoryConfig {
    @Bean
    public ArtifactIndex setArtifactIndex() {
//      return new VolatileArtifactIndex();
      return new SolrArtifactIndex("http://localhost:8983/solr/test");
    }

    @Bean
    public ArtifactDataStore setArtifactDataStore(ArtifactIndex index) throws IOException {
      return new LocalWarcArtifactDataStore(index, LockssTestCase4.getTempDir(tmpDirs));
    }
  }

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  @AfterClass
  public static void deleteTempDirs() throws Exception {
    LockssTestCase4.deleteTempFiles(tmpDirs);
  }

  // *******************************************************************************************************************
  // * TEST UTILITLES
  // *******************************************************************************************************************

  /**
   * Returns a {@link RestLockssRepository} REST client to the
   *
   * @return a LockssRepository with the newly built LOCKSS repository.
   * @throws Exception if there are problems.
   */
  public RestLockssRepository makeRestLockssRepositoryClient() throws Exception {
    URL testEndpoint = new URL(String.format("http://localhost:%d", port));

    log.info("testEndpoint = {}", testEndpoint);

    return new RestLockssRepository(testEndpoint, null, null);
  }

  // *******************************************************************************************************************
  // * TESTS
  // *******************************************************************************************************************

//  @Test
  public void testRestLockssRepository() throws Exception {
    // Create the LOCKSS repository to stress-test
    RestLockssRepository repository = makeRestLockssRepositoryClient();

    LockssRepositoryStressTestContext context = new LockssRepositoryStressTestContext(repository)
        .setCollections(ListUtil.list("collection1", "collection2"))
        .setAuids(ListUtil.list("auid1", "auid2", "auid3"));

    // Start pool of repository operation executors for this repository
    BlockingQueue<LockssRepositoryOperation> ops = new ArrayBlockingQueue(10);

    for (int i = 0; i < 10; i++) {
      Thread exec = new Thread(new LockssRepositoryOperationExecutor(ops));
      exec.start();
    }

    // Create a LOCKSS repository operation builder
    LockssRepositoryOperation.Builder opsBuilder =
        new LockssRepositoryOperation.Builder(context);

    // Queue random repository operations
    while (true) {
      ops.put(opsBuilder.build());
    }
  }

//  @Test
  public void testSmallFiles() throws Exception {
    RestLockssRepository repository = makeRestLockssRepositoryClient();
//    File basePath = LockssTestCase4.getTempDir(tmpDirs);
//    LockssRepository repository = new LocalLockssRepository(basePath, "testIndex");

    LockssRepositoryStressTestContext context = new LockssRepositoryStressTestContext(repository)
        .setCollections(ListUtil.list("collection"))
        .setAuids(ListUtil.list("auid"))
//        .setContentSize(5 * FileUtils.ONE_MB, 20 * FileUtils.ONE_MB);
        .setContentSize(1L, FileUtils.ONE_KB);

    // Create a LOCKSS repository operation builder
    LockssRepositoryOperation.Builder opsBuilder =
        new LockssRepositoryOperation.Builder(context);

    // Start pool of repository operation executors for this repository
    BlockingQueue<LockssRepositoryOperation> ops = new ArrayBlockingQueue(100);

    for (int i = 0; i < 10; i++) {
      Thread t = new Thread(new LockssRepositoryOperationExecutor(ops));
      t.start();
    }

    long numArtifacts = 10000;

    // Queue random repository operations
    for (int i = 0; i < numArtifacts; i++) {
      ops.put(opsBuilder.build(LockssRepositoryOperation.OperationType.ADD_ARTIFACT));
//      ops.put(opsBuilder.build());
    }

    // Wait for queue to empty
    while (!ops.isEmpty()) {
      Thread.sleep(100);
    }

    log.info("WOLF");
  }


  // *******************************************************************************************************************
  // * STRESS-TEST CLASSES
  // *******************************************************************************************************************

  /**
   * Context of a LOCKSS repository stress-test.
   */
  private static class LockssRepositoryStressTestContext {
    private List<Artifact> artifacts = new ArrayList<>();

    private LockssRepository repository;
    private List<String> collections;
    private List<String> auids;

    private Random randomSrc = new Random();

    public LockssRepositoryStressTestContext(LockssRepository repository) {
      this.repository = repository;
    }

    public LockssRepositoryStressTestContext setCollections(List<String> collections) {
      this.collections = collections;
      return this;
    }

    public LockssRepositoryStressTestContext setAuids(List<String> auids) {
      this.auids = auids;
      return this;
    }

    public String randomCollection() {
      return collections.get(randomSrc.nextInt(collections.size()));
    }

    public String randomAuid() {
      return auids.get(randomSrc.nextInt(auids.size()));
    }

    // Holds pre-generated artifact content
    private static List<String> CONTENTS = new ArrayList<>();

    // Pre-compute a set of artifact contents to speed things up
    static {
      for (int i = 1; i<=10; i++) {
        CONTENTS.add(RandomStringUtils.randomAlphabetic((int) (i * FileUtils.ONE_MB)));
      }
    }
    
    long minContentSize = 0;
    long maxContentSize = 0;
    
    public LockssRepositoryStressTestContext setContentSize(long minSize, long maxSize) {
      minContentSize = minSize;
      maxContentSize = maxSize;

      CONTENTS = new ArrayList<>();

      for (int i = 1; i <= 100; i++) {
        CONTENTS
            .add(RandomStringUtils.randomAlphabetic((int) ((maxContentSize - minContentSize) * randomSrc.nextFloat())));
      }

      return this;
    }

    private ArtifactSpec createArtifactSpec() {
      try {
        String content = CONTENTS.get(randomSrc.nextInt(CONTENTS.size()));

        return new ArtifactSpec()
            .setCollection(randomCollection())
            .setAuid(randomAuid())
//            .setUrl("url")
            .setUrl("url" + RandomStringUtils.randomAlphabetic(1))
            .setArtifactId("test") // ignored?
            .setStorageUrl(new URI("test")) // ignored?
            .setCollectionDate(0)
            .setContentLength(content.length())
            .setContent(content);

      } catch (URISyntaxException e) {
        throw new RuntimeException("Unreachable");
      }
    }

    public LockssRepository getRepository() {
      return repository;
    }

    public void addArtifact(Artifact artifact) {
      synchronized (artifacts) {
        artifacts.add(artifact);
      }
    }

    public void removeArtifact(Artifact artifact) {
      synchronized (artifacts) {
        artifacts.remove(artifact);
      }
    }

    public Artifact randomArtifact() {
      synchronized (artifacts) {
        if (artifacts.size() > 0) {
          return artifacts.get(randomSrc.nextInt(artifacts.size()));
        }

        return null;
      }
    }

    public List<Artifact> getArtifacts() {
      return artifacts;
    }

  }

  /**
   * Represents a LOCKSS repository operation.
   */
  private static class LockssRepositoryOperation {

    /**
     * Enum containing types of operations possible and their weights.
     */
    private enum OperationType {
      ADD_ARTIFACT(1),
      GET_ARTIFACT(10),
      COMMIT_ARTIFACT(10),
      DELETE_ARTIFACT(1);

      private static final List<OperationType> DECK = new ArrayList<>();
      private static Random randomSrc = new Random();
      private final int weight;

      OperationType(int weight) {
        this.weight = weight;
      }

      static {
        for (OperationType t : values()) {
          for (int i = 0; i < t.weight; i++) {
            DECK.add(t);
          }
        }
      }

      public static OperationType random() {
        return DECK.get(randomSrc.nextInt(DECK.size()));
      }
    }

    private static class Builder {
      private LockssRepositoryStressTestContext context;

      public Builder(LockssRepositoryStressTestContext context) {
        this.context = context;
      }

      public LockssRepositoryOperation build() {
        return build(OperationType.random());
      }

      private LockssRepositoryOperation build(OperationType operation) {
        return new LockssRepositoryOperation(context, operation);
      }
    }

    private LockssRepositoryStressTestContext context;
    private OperationType operationType;

    /**
     * Constructor.
     *
     * @param context The {@link LockssRepositoryStressTestContext} context to perform the repository operation within.
     * @param operation A {@link OperationType} with the repository operation to perform.
     */
    public LockssRepositoryOperation(LockssRepositoryStressTestContext context, OperationType operation) {
      this.context = context;
      this.operationType = operation;
    }

    /**
     * Returns the context of this repository operation.
     *
     * @return A {@link LockssRepositoryStressTestContext} with the context of this repository operation.
     */
    public LockssRepositoryStressTestContext getContext() {
      return context;
    }

    /**
     * Returns the type of this repository operation.
     *
     * @return A {@link OperationType} indicating the type of this repository operation.
     */
    public OperationType getOperationType() {
      return operationType;
    }

    /**
     * Executes this LOCKSS repository operation.
     *
     * @throws IOException Thrown if there was an I/O exception.
     */
    public void execute() throws IOException {
      log.info("executing = {}", this);

      // Get LOCKSS repository from context
      LockssRepository repository = context.getRepository();

      log.info("repository = {}", repository);

      // Switch execution on operation type
      switch (operationType) {
        case ADD_ARTIFACT: {
          ArtifactSpec spec = context.createArtifactSpec();
          Artifact artifact = repository.addArtifact(spec.getArtifactData());
          log.info("op = {}, artifactId = {}, artifacts = {}", getOperationType().toString(), artifact.getId(),
              context.getArtifacts().size());
          context.addArtifact(artifact);
          break;
        }

        case GET_ARTIFACT: {
          Artifact artifact = context.randomArtifact();
          if (artifact == null) break;
          try {
            log.info("op = {}, artifactId = {}", getOperationType().toString(), artifact.getId());
            repository.getArtifactData(artifact);
          } catch (LockssNoSuchArtifactIdException e) {
            log.info("op = {}, artifactId = {}, MISSING", getOperationType().toString(), artifact.getId());
          }
          break;
        }

        case COMMIT_ARTIFACT: {
          Artifact artifact1 = context.randomArtifact();

          if (artifact1 == null) break;

          log.info("op = {}, artifactId = {}", getOperationType().toString(), artifact1.getId());
          Artifact artifact2 = repository.commitArtifact(artifact1);

          synchronized (context) {
            context.removeArtifact(artifact1);
            context.addArtifact(artifact2);
          }

          break;
        }

        case DELETE_ARTIFACT: {
          Artifact artifact = context.randomArtifact();

          if (artifact == null) break;

          try {
            repository.deleteArtifact(artifact);
            context.removeArtifact(artifact);
            log.info("op = {}, artifactId = {}", getOperationType().toString(), artifact.getId());
          } catch (LockssNoSuchArtifactIdException e) {
            log.info("op = {}, artifactId = {}, MISSING", getOperationType().toString(), artifact.getId());
          }

          break;
        }

        default:
          throw new IllegalStateException("Unknown LOCKSS repository operation");
      }
    }
  }

  /**
   * Executes {@link LockssRepositoryOperation} supplied via a {@link BlockingQueue}.
   */
  private static class LockssRepositoryOperationExecutor implements Runnable {

    /**
     * Handle to LOCKSS repository operations queue.
     */
    BlockingQueue<LockssRepositoryOperation> ops;

    /**
     * Boolean indicating whether the thread is alive.
     */
    boolean isAlive = true;

    /**
     * Constructor.
     *
     * @param ops A {@link BlockingQueue<LockssRepositoryOperation>} queue of repository operations to execute.
     */
    public LockssRepositoryOperationExecutor(BlockingQueue<LockssRepositoryOperation> ops) {
      this.ops = ops;
    }

    /**
     * Stops thread from further execution of {@link LockssRepositoryOperation}s.
     */
    public void stop() {
      isAlive = false;
    }

    /**
     * Implementation of {@link Runnable#run()} which polls for {@link LockssRepositoryOperation}s from the
     * {@link BlockingQueue} and calls their {@link LockssRepositoryOperation#execute()} method.
     */
    @Override
    public void run() {
      while (isAlive) {
        try {
          // Get a repository operation to execute
          LockssRepositoryOperation op = ops.poll(10, TimeUnit.SECONDS);

          // Execute the operation if we got one
          if (op != null) {
            op.execute();
          }

        } catch (IOException e) {
          log.error("Error executing LOCKSS repository operation", e);

        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

}
