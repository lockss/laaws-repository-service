package org.lockss.laaws.rs.impl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.archive.format.warc.WARCConstants;
import org.lockss.laaws.rs.api.ArchivesApiDelegate;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.spring.auth.AuthUtil;
import org.lockss.spring.auth.Roles;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.StringUtil;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ImportStatus;
import org.lockss.util.rest.repo.model.VersionsEnum;
import org.lockss.util.rest.repo.util.ArtifactCache;
import org.lockss.util.rest.repo.util.ImportStatusIterable;
import org.lockss.util.rest.repo.util.NamedInputStreamResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeType;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.jms.JMSException;
import javax.jms.Message;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.GZIPOutputStream;

@Service
public class ArchivesApiServiceImpl extends BaseSpringApiServiceImpl implements ArchivesApiDelegate {
  private static L4JLogger log = L4JLogger.getLogger();
  private final MediaType APPLICATION_WARC = MediaType.valueOf("application/warc");

  /** Maximum length of the AUID-derived part of an exported archive's file name. */
  private static final int MAX_ARCHIVE_FILENAME_LENGTH = 200;

  @Autowired
  LockssRepository repo;

  private final HttpServletRequest request;

  @Autowired
  public ArchivesApiServiceImpl(HttpServletRequest request) {
    this.request = request;
  }

  /**
   * Controller for {@code POST /archives}.
   * <p>
   * Imports the artifacts from an archive into this LOCKSS Repository Service.
   *
   * @param auId         A {@link String} containing the AUID of the artifacts.
   * @param archive      A {@link MultipartFile} containing the archive.
   * @param namespace    A {@link String} containing the namespace of the artifacts.
   * @param storeDuplicate    A {@link Boolean} indicating whether artifacts whose content is identical to the previous version should be stored
   * @param excludeStatusPattern    A {@link String} containing a regexp.  WARC records whose HTTP response status code matches will not be added to the repository
   * @return
   */
  @Override
  public ResponseEntity<Resource> addArtifacts(String auId, MultipartFile archive, String namespace, Boolean storeDuplicate, String excludeStatusPattern) {
    log.debug("archive.name = {}", archive.getName());
    log.debug("archive.origFileName = {}", archive.getOriginalFilename());
    log.debug("archive.type = {}", archive.getContentType());
    if (!StringUtil.isNullString(excludeStatusPattern))
      log.debug("excludeStatusPattern = {}", excludeStatusPattern);

    String parsedRequest = String.format("namespace: %s, auId: %s, requestUrl: %s",
        namespace, auId, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);
    AuthUtil.checkHasRole(Roles.ROLE_AU_ADMIN);

    MimeType archiveType = MimeType.valueOf(archive.getContentType());

    if (archiveType.equals(APPLICATION_WARC)) {
      try {
        boolean needCacheInvalidate = false;

        try (InputStream input = archive.getInputStream();
             ImportStatusIterable result =
                 repo.addArtifacts(namespace, auId, input, LockssRepository.ArchiveType.WARC, storeDuplicate, excludeStatusPattern)) {

          try (DeferredTempFileOutputStream out =
                   new DeferredTempFileOutputStream((int) (16 * FileUtils.ONE_MB), (String) null)) {

            ObjectMapper objMapper = new ObjectMapper();
            objMapper.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
            ObjectWriter objWriter = objMapper.writerFor(ImportStatus.class);

            // Write result to temporary file
            for (ImportStatus status : result) {
              if (ImportStatus.StatusEnum.OK == status.getStatus()) {
                needCacheInvalidate = true;
              }
              objWriter.writeValue(out, status);
            }

            out.flush();

            if (needCacheInvalidate) {
              sendCacheInvalidateAu(ArtifactCache.InvalidateOp.Commit, auId);
            }

            // Set Content-Length of file
            HttpHeaders headers = new HttpHeaders();
            headers.setContentLength(out.getByteCount());

            // Return result as a Resource
            Resource jsonResult = new NamedInputStreamResource("result", out.getDeleteOnCloseInputStream());
            return new ResponseEntity<>(jsonResult, headers, HttpStatus.OK);
          }
        }
      } catch (IllegalArgumentException iae) {
        String message = iae.getMessage();
        log.warn(message);
        log.warn("Parsed request: {}", parsedRequest);
        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.NONE,
            HttpStatus.BAD_REQUEST,
            message, parsedRequest);
      } catch (IOException e) {
        String errorMessage = "Error adding artifacts from archive";
        throw new LockssRestServiceException(LockssRestHttpException.ServerErrorType.APPLICATION_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR, errorMessage, parsedRequest);
      }
    } else {
      String errorMessage = "Archive not supported";
      throw new LockssRestServiceException(LockssRestHttpException.ServerErrorType.NONE,
          HttpStatus.BAD_REQUEST, errorMessage, parsedRequest);
    }
  }


  /**
   * Controller for {@code GET /archives}.
   * <p>
   * Exports the artifacts of an Archival Unit from this LOCKSS Repository Service as an archive.
   * The archive is generated and streamed as the artifacts are read from the repository, so that
   * an AU of any size can be exported without being staged in memory or in a temporary file.
   *
   * @param auId        A {@link String} containing the AUID of the artifacts to export.
   * @param namespace   A {@link String} containing the namespace of the artifacts.
   * @param archiveType A {@link String} containing the type of archive to generate; only
   *                    {@link LockssRepository.ArchiveType#WARC} is supported.
   * @param compress    A {@link Boolean} indicating whether each record of the archive should be
   *                    individually GZIP compressed.
   * @param versions    A {@link String} indicating whether all the versions of each artifact
   *                    ({@code all}) or only the latest ({@code latest}) should be exported.
   * @return a {@link ResponseEntity} streaming the archive.
   */
  @Override
  public ResponseEntity<StreamingResponseBody> exportArtifacts(String auId, String namespace,
                                                               String archiveType, Boolean compress,
                                                               String versions) {
    String parsedRequest = String.format(
        "namespace: %s, auId: %s, archiveType: %s, compress: %s, versions: %s, requestUrl: %s",
        namespace, auId, archiveType, compress, versions,
        ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);
    AuthUtil.checkHasRole(Roles.ROLE_CONTENT_ACCESS, Roles.ROLE_AU_ADMIN);

    if (StringUtil.isNullString(auId)) {
      throw badRequest("Missing AUID", parsedRequest);
    }

    // The import side only reads WARC archives; export only writes them
    if (!LockssRepository.ArchiveType.WARC.name().equalsIgnoreCase(archiveType)) {
      throw badRequest("Archive type not supported: " + archiveType, parsedRequest);
    }

    VersionsEnum versionsEnum = versions == null ?
        VersionsEnum.ALL : VersionsEnum.fromValue(versions.toLowerCase());

    if (versionsEnum == null) {
      throw badRequest("Unknown versions argument: " + versions, parsedRequest);
    }

    boolean gzip = compress != null && compress;

    Iterable<Artifact> artifacts;

    try {
      artifacts = versionsEnum == VersionsEnum.ALL ?
          repo.getArtifactsAllVersions(namespace, auId) :
          repo.getArtifacts(namespace, auId);
    } catch (IllegalArgumentException iae) {
      throw badRequest(iae.getMessage(), parsedRequest);
    } catch (IOException e) {
      String errorMessage = "Error getting the artifacts of the Archival Unit";
      log.error(errorMessage, e);
      throw new LockssRestServiceException(LockssRestHttpException.ServerErrorType.APPLICATION_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR, errorMessage, parsedRequest);
    }

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(APPLICATION_WARC);
    headers.setContentDisposition(ContentDisposition.attachment()
        .filename(getArchiveFileName(auId, gzip))
        .build());

    // The archive is written by the StreamingResponseBody, i.e., after the response headers have
    // been sent: an error encountered while writing it can only truncate the response.
    return new ResponseEntity<>(output -> writeArchive(artifacts, gzip, output),
        headers, HttpStatus.OK);
  }

  /**
   * Writes the artifacts of an Archival Unit to an {@link OutputStream} as a WARC archive.
   *
   * @param artifacts An {@code Iterable<Artifact>} over the artifacts to write.
   * @param gzip      A {@code boolean} indicating whether each WARC record should be individually
   *                  GZIP compressed.
   * @param output    The {@link OutputStream} to write the archive to.
   * @throws IOException if there are problems reading the artifacts or writing the archive.
   */
  private void writeArchive(Iterable<Artifact> artifacts, boolean gzip, OutputStream output)
      throws IOException {

    // The index returns the versions of a URL in decreasing order; write them oldest version
    // first, so that the archive reads chronologically and so that an import of it sees the
    // previous version of an artifact before the artifact itself, which is what its duplicate
    // suppression compares against. Only the artifacts of one URL are buffered, and only their
    // metadata.
    List<Artifact> urlVersions = new ArrayList<>();
    String url = null;

    long numArtifacts = 0;

    for (Artifact artifact : artifacts) {
      if (!artifact.getUri().equals(url)) {
        numArtifacts += writeUrlVersions(urlVersions, gzip, output);
        urlVersions.clear();
        url = artifact.getUri();
      }

      urlVersions.add(artifact);
    }

    numArtifacts += writeUrlVersions(urlVersions, gzip, output);

    output.flush();

    log.debug2("Wrote {} artifacts to archive", numArtifacts);
  }

  /**
   * Writes the versions of a single URL, oldest version first.
   *
   * @param urlVersions A {@code List<Artifact>} containing the versions of one URL, in decreasing
   *                    version order.
   * @param gzip        A {@code boolean} indicating whether each WARC record should be individually
   *                    GZIP compressed.
   * @param output      The {@link OutputStream} to write the WARC records to.
   * @return the number of artifacts written.
   * @throws IOException if there are problems reading the artifacts or writing the records.
   */
  private long writeUrlVersions(List<Artifact> urlVersions, boolean gzip, OutputStream output)
      throws IOException {

    for (int i = urlVersions.size() - 1; i >= 0; i--) {
      writeArtifact(urlVersions.get(i), gzip, output);
    }

    return urlVersions.size();
  }

  /**
   * Writes a single artifact to an {@link OutputStream} as a WARC record.
   *
   * @param artifact The {@link Artifact} to write.
   * @param gzip     A {@code boolean} indicating whether the WARC record should be GZIP compressed.
   * @param output   The {@link OutputStream} to write the WARC record to.
   * @throws IOException if there are problems reading the artifact or writing the record.
   */
  private void writeArtifact(Artifact artifact, boolean gzip, OutputStream output)
      throws IOException {

    try (ArtifactData artifactData = repo.getArtifactData(artifact)) {
      // WarcArtifactDataStore#writeArtifactData() closes the OutputStream it is given, which would
      // end the response after the first record: shield the stream from being closed by it.
      if (gzip) {
        // A GZIP member per record, as in the repository's own compressed WARC files
        try (GZIPOutputStream gzipOutput =
                 new GZIPOutputStream(CloseShieldOutputStream.wrap(output))) {
          WarcArtifactDataStore.writeArtifactData(artifactData,
              CloseShieldOutputStream.wrap(gzipOutput));
        }
      } else {
        WarcArtifactDataStore.writeArtifactData(artifactData,
            CloseShieldOutputStream.wrap(output));
      }
    }
  }

  /**
   * Provides the file name to suggest to the client for the archive of an Archival Unit.
   *
   * @param auId A {@link String} containing the AUID of the exported Archival Unit.
   * @param gzip A {@code boolean} indicating whether the archive is GZIP compressed.
   * @return a {@link String} containing the file name.
   */
  private String getArchiveFileName(String auId, boolean gzip) {
    // AUIDs contain characters that don't belong in a file name
    String name = auId.replaceAll("[^A-Za-z0-9._-]", "_");

    if (name.length() > MAX_ARCHIVE_FILENAME_LENGTH) {
      name = name.substring(0, MAX_ARCHIVE_FILENAME_LENGTH);
    }

    return name + (gzip ?
        WARCConstants.DOT_COMPRESSED_WARC_FILE_EXTENSION :
        WARCConstants.DOT_WARC_FILE_EXTENSION);
  }

  /**
   * Provides a {@link LockssRestServiceException} reporting a bad request.
   *
   * @param message       A {@link String} containing the error message.
   * @param parsedRequest A {@link String} containing the parsed request, for diagnostic purposes.
   * @return the {@link LockssRestServiceException} to be thrown.
   */
  private LockssRestServiceException badRequest(String message, String parsedRequest) {
    log.warn(message);
    log.warn("Parsed request: {}", parsedRequest);

    return new LockssRestServiceException(LockssRestHttpException.ServerErrorType.NONE,
        HttpStatus.BAD_REQUEST, message, parsedRequest);
  }

  @jakarta.annotation.PostConstruct
  private void init() {
    setUpJms(JMS_SEND,
        RestLockssRepository.REST_ARTIFACT_CACHE_ID,
        RestLockssRepository.REST_ARTIFACT_CACHE_TOPIC);
  }

  protected void sendCacheInvalidateAu(ArtifactCache.InvalidateOp op,
                                       String auid) {
    if (jmsProducer != null && auid != null) {
      Map<String, Object> map = new HashMap<>();
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION,
          RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_INVALIDATE_AU);
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_OP, op.toString());
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_KEY, auid);
      try {
        jmsProducer.sendMap(map);
      } catch (JMSException e) {
        log.error("Couldn't send cache invalidate notification", e);
      }
    }
  }

}
