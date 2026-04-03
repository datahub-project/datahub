package io.datahubproject.openapi.v1.documents;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import com.linkedin.metadata.service.docimport.ImportResult;
import com.linkedin.metadata.service.docimport.ImportUseCase;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * REST controller for importing documents from uploaded files. Performs text extraction and
 * document creation directly in GMS (no external service dependency).
 */
@RestController
@RequestMapping("/openapi/v1/documents")
@Tag(name = "Document Import", description = "Import documents from uploaded files")
@Slf4j
public class DocumentImportController {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final DocumentImportService importService;
  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;

  @Autowired
  public DocumentImportController(
      DocumentImportService importService,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain) {
    this.importService = importService;
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
  }

  @PostMapping(
      value = "/import/files",
      consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Import documents from uploaded files",
      description =
          "Upload text files (.md, .txt, .pdf, .docx, .html) to create context documents. "
              + "Each file becomes one document.")
  public ResponseEntity<String> importFromFiles(
      @RequestParam("files") List<MultipartFile> files,
      @RequestParam(value = "showInGlobalContext", defaultValue = "true")
          boolean showInGlobalContext,
      @RequestParam(value = "useCase", defaultValue = "CONTEXT_DOCUMENT") String useCase,
      @RequestParam(value = "parentDocumentUrn", required = false) String parentDocumentUrn,
      HttpServletRequest request) {

    final Authentication authentication = AuthenticationContext.getAuthentication();
    if (authentication == null) {
      throw new UnauthorizedException("Authentication required");
    }

    final OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    request,
                    "importDocuments",
                    Collections.singleton(Constants.DOCUMENT_ENTITY_NAME)),
            authorizerChain,
            authentication,
            true);
    if (!AuthUtil.isAuthorized(opContext, PoliciesConfig.MANAGE_DOCUMENTS_PRIVILEGE)) {
      throw new UnauthorizedException(
          "Unauthorized to import documents. Requires MANAGE_DOCUMENTS privilege.");
    }

    if (files == null || files.isEmpty()) {
      return ResponseEntity.badRequest().body("{\"error\": \"No files provided\"}");
    }

    try {
      final Urn actorUrn = Urn.createFromString(authentication.getActor().toUrnStr());

      List<Map.Entry<String, byte[]>> fileData = new ArrayList<>();
      for (MultipartFile file : files) {
        if (file.getOriginalFilename() != null && !file.isEmpty()) {
          fileData.add(new AbstractMap.SimpleEntry<>(file.getOriginalFilename(), file.getBytes()));
        }
      }

      if (fileData.isEmpty()) {
        return ResponseEntity.badRequest().body("{\"error\": \"No valid files provided\"}");
      }

      Urn parentUrn = parentDocumentUrn != null ? Urn.createFromString(parentDocumentUrn) : null;

      ImportResult result =
          importService.importFromFiles(
              opContext,
              fileData,
              ImportUseCase.fromString(useCase),
              showInGlobalContext,
              parentUrn,
              actorUrn);

      return ResponseEntity.ok(MAPPER.writeValueAsString(result));
    } catch (Exception e) {
      log.error("Failed to import documents from files", e);
      return ResponseEntity.internalServerError()
          .body(
              String.format(
                  "{\"error\": \"Failed to import documents: %s\"}",
                  e.getMessage().replace("\"", "\\\"")));
    }
  }

  @GetMapping(value = "/import/supported-formats", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get supported file formats",
      description = "Returns the list of file extensions supported for document import.")
  public ResponseEntity<Map<String, List<String>>> getSupportedFormats() {
    return ResponseEntity.ok(Map.of("extensions", importService.getSupportedFileExtensions()));
  }
}
