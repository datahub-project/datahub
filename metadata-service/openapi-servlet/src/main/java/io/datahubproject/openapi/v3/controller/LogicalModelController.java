package io.datahubproject.openapi.v3.controller;

import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.logical.LogicalModelUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.util.MappingUtil;
import io.datahubproject.openapi.v3.models.GenericEntityV3;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController("LogicalModelController")
@RequiredArgsConstructor
@RequestMapping("/openapi/v3/logical")
@Slf4j
@Tag(name = "Logical Models", description = "APIs for interacting with logical models.")
public class LogicalModelController {

  @Autowired protected EntityService<?> entityService;
  @Autowired protected AuthorizerChain authorizationChain;
  @Autowired protected ObjectMapper objectMapper;

  @Qualifier("systemOperationContext")
  @Autowired
  protected OperationContext systemOperationContext;

  @Tag(name = "Logical Models")
  @PostMapping(
      value = "{childDatasetUrn}/relationship/physicalInstanceOf/{parentDatasetUrn}",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Associate a physical dataset and its schema fields to a logical dataset")
  public ResponseEntity<List<GenericEntityV3>> setLogicalParents(
      HttpServletRequest request,
      @PathVariable("childDatasetUrn") String childDatasetUrnStr,
      @PathVariable("parentDatasetUrn") String parentDatasetUrnStr,
      @RequestBody String jsonBody)
      throws JsonProcessingException {
    return setLogicalParentsHelper(
        request,
        childDatasetUrnStr,
        parentDatasetUrnStr,
        jsonBody,
        entityService,
        authorizationChain,
        objectMapper,
        systemOperationContext);
  }

  // Remove when EntityController methods are deprecated
  public static ResponseEntity<List<GenericEntityV3>> setLogicalParentsHelper(
      HttpServletRequest request,
      @PathVariable("childDatasetUrn") String childDatasetUrnStr,
      @PathVariable("parentDatasetUrn") String parentDatasetUrnStr,
      @RequestBody String jsonBody,
      EntityService<?> entityService,
      AuthorizerChain authorizationChain,
      ObjectMapper objectMapper,
      OperationContext systemOperationContext)
      throws JsonProcessingException {
    // Expect map of parent field path to child field path
    Map<String, String> fieldPathMap = objectMapper.readValue(jsonBody, new TypeReference<>() {});

    Authentication authentication = AuthenticationContext.getAuthentication();
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);
    Urn parentDatasetUrn = UrnUtils.getUrn(parentDatasetUrnStr);
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    request,
                    "setLogicalParents",
                    ImmutableSet.of(
                        childDatasetUrn.getEntityType(), parentDatasetUrn.getEntityType())),
            authorizationChain,
            authentication,
            true);

    // Assumes if a user has access to a dataset, they have access to its schema fields
    if (!AuthUtil.isAPIAuthorizedEntityUrns(
        opContext, UPDATE, ImmutableSet.of(childDatasetUrn, parentDatasetUrn))) {
      throw new UnauthorizedException(
          String.format(
              "%s is unauthorized to %s entities %s and %s",
              authentication.getActor().toUrnStr(),
              UPDATE,
              childDatasetUrnStr,
              parentDatasetUrnStr));
    }

    // Validate all field paths exist, if schema metadata aspects exist
    RecordTemplate parentSchemaMetadataAspect =
        entityService.getLatestAspect(
            opContext, parentDatasetUrn, Constants.SCHEMA_METADATA_ASPECT_NAME);
    RecordTemplate childSchemaMetadataAspect =
        entityService.getLatestAspect(
            opContext, childDatasetUrn, Constants.SCHEMA_METADATA_ASPECT_NAME);

    if (parentSchemaMetadataAspect != null && childSchemaMetadataAspect != null) {
      SchemaMetadata parentSchema = (SchemaMetadata) parentSchemaMetadataAspect;
      SchemaMetadata childSchema = (SchemaMetadata) childSchemaMetadataAspect;

      Set<String> childFieldPaths =
          childSchema.getFields().stream()
              .map(SchemaField::getFieldPath)
              .collect(Collectors.toSet());
      Set<String> parentFieldPaths =
          parentSchema.getFields().stream()
              .map(SchemaField::getFieldPath)
              .collect(Collectors.toSet());

      for (Map.Entry<String, String> mapping : fieldPathMap.entrySet()) {
        if (!parentFieldPaths.contains(mapping.getKey())) {
          throw new IllegalArgumentException(
              String.format(
                  "Field path not found on parent %s: %s", parentDatasetUrnStr, mapping.getKey()));
        }
        if (!childFieldPaths.contains(mapping.getValue())) {
          throw new IllegalArgumentException(
              String.format(
                  "Field path not found on child %s: %s", childDatasetUrnStr, mapping.getValue()));
        }
      }
    }

    // Create logical parent proposals
    List<MetadataChangeProposal> proposals = new ArrayList<>();

    // Create dataset -> dataset logical parent aspect
    proposals.add(
        LogicalModelUtils.createLogicalParentProposal(
            childDatasetUrn, parentDatasetUrn, opContext));

    // Create schema field -> schema field logical parent aspects
    for (Map.Entry<String, String> mapping : fieldPathMap.entrySet()) {
      Urn parentSchemaFieldUrn =
          SchemaFieldUtils.generateSchemaFieldUrn(parentDatasetUrn, mapping.getKey());
      Urn childSchemaFieldUrn =
          SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, mapping.getValue());
      proposals.add(
          LogicalModelUtils.createLogicalParentProposal(
              childSchemaFieldUrn, parentSchemaFieldUrn, opContext));
    }

    // Batch ingest all proposals
    AuditStamp auditStamp = AuditStampUtils.createAuditStamp(authentication.getActor().toUrnStr());
    AspectsBatch batch =
        AspectsBatchImpl.builder()
            .mcps(proposals, auditStamp, opContext.getRetrieverContext())
            .build(opContext);
    List<IngestResult> results = entityService.ingestProposal(opContext, batch, false);
    return ResponseEntity.ok(MappingUtil.buildGenericEntityV3List(objectMapper, results, false));
  }

  @Tag(name = "Logical Models")
  @DeleteMapping(
      value = "{childDatasetUrn}/relationship/physicalInstanceOf",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary =
          "Remove the associations between a physical and a logical dataset, including their schema fields")
  public ResponseEntity<List<GenericEntityV3>> removeLogicalParents(
      HttpServletRequest request, @PathVariable("childDatasetUrn") String childDatasetUrnStr) {
    return removeLogicalParentsHelper(
        request,
        childDatasetUrnStr,
        entityService,
        authorizationChain,
        objectMapper,
        systemOperationContext);
  }

  // Remove when EntityController methods are deprecated
  public static ResponseEntity<List<GenericEntityV3>> removeLogicalParentsHelper(
      HttpServletRequest request,
      @PathVariable("childDatasetUrn") String childDatasetUrnStr,
      EntityService<?> entityService,
      AuthorizerChain authorizationChain,
      ObjectMapper objectMapper,
      OperationContext systemOperationContext) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    Urn childDatasetUrn = UrnUtils.getUrn(childDatasetUrnStr);
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(),
                    request,
                    "removeLogicalParents",
                    ImmutableSet.of(childDatasetUrn.getEntityType())),
            authorizationChain,
            authentication,
            true);

    // Assumes if a user has access to a dataset, they have access to its schema fields
    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, UPDATE, ImmutableSet.of(childDatasetUrn))) {
      throw new UnauthorizedException(
          String.format(
              "%s is unauthorized to %s entity %s",
              authentication.getActor().toUrnStr(), UPDATE, childDatasetUrnStr));
    }

    // Validate all field paths exist, if schema metadata aspects exist
    RecordTemplate childSchemaMetadataAspect =
        entityService.getLatestAspect(
            opContext, childDatasetUrn, Constants.SCHEMA_METADATA_ASPECT_NAME);

    // Remove logical parent proposals
    List<MetadataChangeProposal> proposals = new ArrayList<>();

    // Unset dataset -> dataset logical parent
    proposals.add(LogicalModelUtils.createLogicalParentProposal(childDatasetUrn, null, opContext));

    if (childSchemaMetadataAspect != null) {
      // Unset column -> column logical parents
      SchemaMetadata childSchema = (SchemaMetadata) childSchemaMetadataAspect;
      childSchema.getFields().stream()
          .map(SchemaField::getFieldPath)
          .forEach(
              fieldPath ->
                  proposals.add(
                      LogicalModelUtils.createLogicalParentProposal(
                          SchemaFieldUtils.generateSchemaFieldUrn(childDatasetUrn, fieldPath),
                          null,
                          opContext)));
    }

    // Batch ingest all proposals
    AuditStamp auditStamp = AuditStampUtils.createAuditStamp(authentication.getActor().toUrnStr());
    AspectsBatch batch =
        AspectsBatchImpl.builder()
            .mcps(proposals, auditStamp, opContext.getRetrieverContext())
            .build(opContext);
    List<IngestResult> results = entityService.ingestProposal(opContext, batch, false);
    return ResponseEntity.ok(MappingUtil.buildGenericEntityV3List(objectMapper, results, false));
  }
}
