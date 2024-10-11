package com.linkedin.metadata.service;

import static com.linkedin.metadata.entity.AspectUtils.*;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.openapi.v2.models.BatchGetUrnRequestV2;
import io.datahubproject.openapi.v2.models.BatchGetUrnResponseV2;
import io.datahubproject.openapi.v2.models.GenericAspectV2;
import io.datahubproject.openapi.v2.models.GenericEntityV2;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseService {

  protected final ObjectMapper objectMapper;
  protected final SystemEntityClient entityClient;
  protected final OpenApiClient openApiClient;

  public BaseService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    this.entityClient = Objects.requireNonNull(entityClient);
    this.openApiClient = openApiClient;
    this.objectMapper = objectMapper;
  }

  @Nonnull
  protected Map<Urn, GlobalTags> getTagsAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> entityUrns,
      @Nonnull GlobalTags defaultValue) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      Map<Urn, Aspect> aspects =
          batchGetLatestAspect(
              opContext,
              entityUrns.stream().findFirst().get().getEntityType(), // TODO Improve this.
              entityUrns,
              Constants.GLOBAL_TAGS_ASPECT_NAME,
              this.entityClient);

      final Map<Urn, GlobalTags> finalResult = new HashMap<>();
      if (aspects != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspects.get(entity);
          if (aspect == null) {
            finalResult.put(entity, defaultValue);
          } else {
            finalResult.put(entity, new GlobalTags(aspect.data()));
          }
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving global tags for {} entities. Sample Urns: {} aspect: {}",
          entityUrns.size(),
          entityUrns.stream().limit(10).collect(Collectors.toList()),
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          e);
      return Collections.emptyMap();
    }
  }

  @Nullable
  protected Map<Urn, RecordTemplate> getAspectMap(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> entityUrns, String aspectName) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      BatchGetUrnRequestV2 getUrnRequest =
          BatchGetUrnRequestV2.builder()
              .urns(entityUrns.stream().map(Urn::toString).collect(Collectors.toList()))
              .aspectNames(Collections.singletonList(aspectName))
              .withSystemMetadata(true)
              .build();
      BatchGetUrnResponseV2<GenericAspectV2, GenericEntityV2> response =
          openApiClient.getBatchUrns(
              entityUrns.stream().findFirst().get().getEntityType(),
              getUrnRequest,
              opContext.getSessionAuthentication().getCredentials());
      log.info("BatchGetUrnResponse: number of entities {}", response.getEntities().size());
      log.debug("BatchGetUrnResponse: {}", response);
      return response.getEntities().stream()
          .filter(entity -> entity.getAspects().get(aspectName) != null)
          .collect(
              Collectors.toMap(
                  entity -> UrnUtils.getUrn(entity.getUrn()),
                  entity -> convertToRecordTemplate(entity, aspectName)));

    } catch (Exception e) {
      log.error("Error retrieving {} for entities. Entities: {}", aspectName, entityUrns, e);
      return null;
    }
  }

  private RecordTemplate convertToRecordTemplate(GenericEntityV2 entity, String aspectName) {
    JsonNode valueNode = objectMapper.valueToTree(entity.getAspects().get(aspectName)).get("value");
    AspectSpec aspectSpec =
        openApiClient
            .getSystemOperationContext()
            .getEntityRegistry()
            .getEntitySpec(UrnUtils.getUrn(entity.getUrn()).getEntityType())
            .getAspectSpec(aspectName);
    if (valueNode == null) {
      throw new IllegalStateException(
          "Value for aspect " + aspectName + " for entity " + entity + " must not be null.");
    }
    return RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(), valueNode.toString());
  }

  @Nonnull
  protected Map<Urn, EditableSchemaMetadata> getEditableSchemaMetadataAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> entityUrns,
      @Nonnull EditableSchemaMetadata defaultValue) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      Map<Urn, Aspect> aspects =
          batchGetLatestAspect(
              opContext,
              entityUrns.stream().findFirst().get().getEntityType(), // TODO Improve this.
              entityUrns,
              Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
              this.entityClient);

      final Map<Urn, EditableSchemaMetadata> finalResult = new HashMap<>();
      if (aspects != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspects.get(entity);
          if (aspect == null) {
            finalResult.put(entity, defaultValue);
          } else {
            finalResult.put(entity, new EditableSchemaMetadata(aspect.data()));
          }
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving editable schema metadata for entities. Entities: {} aspect: {}",
          entityUrns,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          e);
      return Collections.emptyMap();
    }
  }

  @Nonnull
  protected Map<Urn, Ownership> getOwnershipAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> entityUrns,
      @Nonnull Ownership defaultValue) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      Map<Urn, Aspect> aspects =
          batchGetLatestAspect(
              opContext,
              entityUrns.stream().findFirst().get().getEntityType(), // TODO Improve this.
              entityUrns,
              Constants.OWNERSHIP_ASPECT_NAME,
              this.entityClient);

      final Map<Urn, Ownership> finalResult = new HashMap<>();
      if (aspects != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspects.get(entity);
          if (aspect == null) {
            finalResult.put(entity, defaultValue);
          } else {
            finalResult.put(entity, new Ownership(aspect.data()));
          }
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving ownership for entities. Entities: {} aspect: {}",
          entityUrns,
          Constants.OWNERSHIP_ASPECT_NAME,
          e);
      return Collections.emptyMap();
    }
  }

  @Nonnull
  protected Map<Urn, GlossaryTerms> getGlossaryTermsAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> entityUrns,
      @Nonnull GlossaryTerms defaultValue) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      Map<Urn, Aspect> aspects =
          batchGetLatestAspect(
              opContext,
              entityUrns.stream().findFirst().get().getEntityType(), // TODO Improve this.
              entityUrns,
              Constants.GLOSSARY_TERMS_ASPECT_NAME,
              this.entityClient);

      final Map<Urn, GlossaryTerms> finalResult = new HashMap<>();
      if (aspects != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspects.get(entity);
          if (aspect == null) {
            finalResult.put(entity, defaultValue);
          } else {
            finalResult.put(entity, new GlossaryTerms(aspect.data()));
          }
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving glossary terms for entities. Entities: {} aspect: {}",
          entityUrns,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          e);
      return Collections.emptyMap();
    }
  }

  @Nonnull
  protected Map<Urn, Domains> getDomainsAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> entityUrns,
      @Nonnull Domains defaultValue) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      Map<Urn, Aspect> aspects =
          batchGetLatestAspect(
              opContext,
              entityUrns.stream().findFirst().get().getEntityType(), // TODO Improve this.
              entityUrns,
              Constants.DOMAINS_ASPECT_NAME,
              this.entityClient);

      final Map<Urn, Domains> finalResult = new HashMap<>();
      if (aspects != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspects.get(entity);
          if (aspect == null) {
            finalResult.put(entity, defaultValue);
          } else {
            finalResult.put(entity, new Domains(aspect.data()));
          }
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving domains for entities. Entities: {} aspect: {}",
          entityUrns,
          Constants.DOMAINS_ASPECT_NAME,
          e);
      return Collections.emptyMap();
    }
  }

  protected void ingestChangeProposals(
      @Nonnull OperationContext opContext, @Nonnull List<MetadataChangeProposal> changes) {
    try {
      this.entityClient.batchIngestProposals(opContext, changes, false);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(e);
    }
  }

  protected void ingestChangeProposals(
      @Nonnull OperationContext opContext,
      @Nonnull List<MetadataChangeProposal> changes,
      final boolean async) {
    try {
      this.entityClient.batchIngestProposals(opContext, changes, async);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(e);
    }
  }
}
