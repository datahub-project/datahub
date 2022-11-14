package io.datahubproject.openapi.platform.entities;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.generated.MetadataChangeProposal;
import io.datahubproject.openapi.util.MappingUtil;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
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
      @RequestBody @Nonnull List<MetadataChangeProposal> metadataChangeProposals) {
    log.info("INGEST PROPOSAL proposal: {}", metadataChangeProposals);

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    List<Pair<String, Boolean>> responses = metadataChangeProposals.stream()
        .map(proposal -> MappingUtil.ingestProposal(proposal, actorUrnStr, _entityService, _objectMapper))
        .collect(Collectors.toList());
    if (responses.stream().anyMatch(Pair::getSecond)) {
      return ResponseEntity.status(HttpStatus.CREATED)
          .body(responses.stream().filter(Pair::getSecond).map(Pair::getFirst).collect(Collectors.toList()));
    } else {
      return ResponseEntity.ok(Collections.emptyList());
    }
  }
}
