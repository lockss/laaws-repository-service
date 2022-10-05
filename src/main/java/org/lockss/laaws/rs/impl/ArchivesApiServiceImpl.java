package org.lockss.laaws.rs.impl;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.io.FileUtils;
import org.archive.format.warc.WARCConstants;
import org.lockss.laaws.rs.api.ArchivesApiDelegate;
import org.lockss.laaws.rs.core.ImportStatusIterable;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.model.ImportStatus;
import org.lockss.laaws.rs.util.NamedInputStreamResource;
import org.lockss.log.L4JLogger;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.StringUtil;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MimeType;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;

public class ArchivesApiServiceImpl extends BaseSpringApiServiceImpl implements ArchivesApiDelegate {
  private static L4JLogger log = L4JLogger.getLogger();
  private final MediaType APPLICATION_WARC = MediaType.valueOf("application/warc");

  @Autowired
  LockssRepository repo;

  private final HttpServletRequest request;

  @Autowired
  public ArchivesApiServiceImpl(HttpServletRequest request) {
    this.request = request;
  }

  /**
   * Controller for {@code POST /collections/{collectionId}/archives}.
   * <p>
   * Imports the artifacts from an archive into this LOCKSS Repository Service.
   *
   * @param namespaces A {@link String} containing the collection ID of the artifacts.
   * @param auId         A {@link String} containing the AUID of the artifacts.
   * @param archive      A {@link MultipartFile} containing the archive.
   * @return
   */
  @Override
  public ResponseEntity<Resource> addArtifacts(String auId, MultipartFile archive, String namespaces) {
    log.debug("archive.name = {}", archive.getName());
    log.debug("archive.origFileName = {}", archive.getOriginalFilename());
    log.debug("archive.type = {}", archive.getContentType());

    String parsedRequest = String.format("collectionId: %s, auId: %s, requestUrl: %s",
        namespaces, auId, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    MimeType archiveType = MimeType.valueOf(archive.getContentType());

    if (archiveType.equals(APPLICATION_WARC)) {
      try {
        boolean isCompressed = StringUtil.endsWithIgnoreCase(
            archive.getOriginalFilename(), WARCConstants.DOT_COMPRESSED_WARC_FILE_EXTENSION);

        try (InputStream input = archive.getInputStream();
             ImportStatusIterable result =
                 repo.addArtifacts(namespaces, auId, input, LockssRepository.ArchiveType.WARC, isCompressed)) {

          try (DeferredTempFileOutputStream out =
                   new DeferredTempFileOutputStream((int) (16 * FileUtils.ONE_MB), null)) {

            ObjectMapper objMapper = new ObjectMapper();
            objMapper.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
            ObjectWriter objWriter = objMapper.writerFor(ImportStatus.class);

            // Write result to temporary file
            for (ImportStatus status : result) {
              objWriter.writeValue(out, status);
            }

            out.flush();

            // Set Content-Length of file
            HttpHeaders headers = new HttpHeaders();
            headers.setContentLength(out.getByteCount());

            // Return result as a Resource
            Resource jsonResult = new NamedInputStreamResource("result", out.getDeleteOnCloseInputStream());
            return new ResponseEntity<>(jsonResult, headers, HttpStatus.OK);
          }
        }
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

}
