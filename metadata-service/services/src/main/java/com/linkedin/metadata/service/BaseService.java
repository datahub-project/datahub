package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
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
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaMetadata;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.openapi.v2.models.BatchGetUrnRequest;
import io.datahubproject.openapi.v2.models.BatchGetUrnResponse;
import io.datahubproject.openapi.v2.models.GenericEntity;
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

  protected final EntityClient entityClient;
  protected final OpenApiClient openApiClient;
  protected final Authentication systemAuthentication;

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public BaseService(
      @Nonnull EntityClient entityClient,
      @Nonnull Authentication systemAuthentication,
      @Nonnull OpenApiClient openApiClient) {
    this.entityClient = Objects.requireNonNull(entityClient);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
    this.openApiClient = openApiClient;
  }

  @Nonnull
  protected Map<Urn, GlobalTags> getTagsAspects(
      @Nonnull Set<Urn> entityUrns,
      @Nonnull GlobalTags defaultValue,
      @Nonnull Authentication authentication) {
    try {
      Map<Urn, RecordTemplate> aspectMap =
          getAspectMap(entityUrns, Constants.GLOBAL_TAGS_ASPECT_NAME, authentication);

      final Map<Urn, GlobalTags> finalResult = new HashMap<>();
      if (aspectMap != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspectMap.get(entity);
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
          "Error retrieving global tags for entities. Entities: {} aspect: {}",
          entityUrns,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          e);
      return Collections.emptyMap();
    }
  }

  @Nullable
  protected Map<Urn, RecordTemplate> getAspectMap(
      @Nonnull Set<Urn> entityUrns, String aspectName, @Nonnull Authentication authentication) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      BatchGetUrnRequest getUrnRequest =
          BatchGetUrnRequest.builder()
              .urns(entityUrns.stream().map(Urn::toString).collect(Collectors.toList()))
              .aspectNames(Collections.singletonList(aspectName))
              .withSystemMetadata(true)
              .build();
      BatchGetUrnResponse response =
          openApiClient.getBatchUrns(
              entityUrns.stream().findFirst().get().getEntityType(),
              getUrnRequest,
              authentication.getCredentials());
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

  private RecordTemplate convertToRecordTemplate(GenericEntity entity, String aspectName) {
    JsonNode valueNode =
        OBJECT_MAPPER.valueToTree(entity.getAspects().get(aspectName)).get("value");
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
      @Nonnull Set<Urn> entityUrns,
      @Nonnull EditableSchemaMetadata defaultValue,
      @Nonnull Authentication authentication) {

    try {
      Map<Urn, RecordTemplate> aspectMap =
          getAspectMap(entityUrns, Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME, authentication);

      final Map<Urn, EditableSchemaMetadata> finalResult = new HashMap<>();
      if (aspectMap != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspectMap.get(entity);
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
      @Nonnull Set<Urn> entityUrns,
      @Nonnull Ownership defaultValue,
      @Nonnull Authentication authentication) {
    try {
      Map<Urn, RecordTemplate> aspectMap =
          getAspectMap(entityUrns, Constants.OWNERSHIP_ASPECT_NAME, authentication);

      final Map<Urn, Ownership> finalResult = new HashMap<>();
      if (aspectMap != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspectMap.get(entity);
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
      @Nonnull Set<Urn> entityUrns,
      @Nonnull GlossaryTerms defaultValue,
      @Nonnull Authentication authentication) {
    try {
      Map<Urn, RecordTemplate> aspectMap =
          getAspectMap(entityUrns, Constants.GLOSSARY_TERMS_ASPECT_NAME, authentication);

      final Map<Urn, GlossaryTerms> finalResult = new HashMap<>();
      if (aspectMap != null) {
        for (Urn entity : entityUrns) {
          RecordTemplate aspect = aspectMap.get(entity);
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
      @Nonnull Set<Urn> entityUrns,
      @Nonnull Domains defaultValue,
      @Nonnull Authentication authentication) {
    try {
      Map<Urn, RecordTemplate> aspects =
          getAspectMap(entityUrns, Constants.DOMAINS_ASPECT_NAME, authentication);

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
      @Nonnull List<MetadataChangeProposal> changes, @Nonnull Authentication authentication)
      throws Exception {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      this.entityClient.ingestProposal(change, authentication);
    }
  }
}
