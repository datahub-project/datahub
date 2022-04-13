package io.datahubproject.openapi.platform.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.openapi.generated.MetadataChangeProposal;
import io.datahubproject.openapi.util.MappingUtil;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@AllArgsConstructor
@RequestMapping("/platform/entities/v1")
@Slf4j
@Tag(name = "Platform Entities", description = "Platform level APIs intended for lower level access to entities")
public class PlatformEntitiesController {

  private final EntityService _entityService;
  private final ObjectMapper _objectMapper;

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @PostMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<String>> postEntities(
      @RequestBody @Nonnull List<MetadataChangeProposal> metadataChangeProposal) {
    log.info("INGEST PROPOSAL proposal: {}", metadataChangeProposal);

    return ResponseEntity.ok(metadataChangeProposal.stream()
        .map(proposal -> MappingUtil.ingestProposal(proposal, _entityService, _objectMapper))
        .collect(Collectors.toList()));
  }
}
