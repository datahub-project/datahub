package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.entity.AspectUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

  @Nullable
  protected EditableSchemaFieldInfo getEditableSchemaField(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final EditableSchemaMetadata maybeEditableSchemaMetadata =
        getEditableSchemaMetadata(opContext, entityUrn);
    if (maybeEditableSchemaMetadata != null) {
      return extractSchemaFieldFromEditableSchemaMetadata(maybeEditableSchemaMetadata, fieldPath);
    }
    return null;
  }

  @Nullable
  protected SchemaField getSchemaField(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final String fieldPath) {
    final SchemaMetadata maybeSchemaMetadata = getSchemaMetadata(opContext, entityUrn);
    if (maybeSchemaMetadata != null) {
      return extractSchemaFieldFromSchemaMetadata(maybeSchemaMetadata, fieldPath);
    }
    return null;
  }

  @Nullable
  private EditableSchemaFieldInfo extractSchemaFieldFromEditableSchemaMetadata(
      @Nonnull final EditableSchemaMetadata editableSchemaMetadata,
      @Nonnull final String fieldPath) {
    return editableSchemaMetadata.getEditableSchemaFieldInfo().stream()
        .filter(field -> fieldPath.equals(field.getFieldPath()))
        .findFirst()
        .orElse(null);
  }

  @Nullable
  private SchemaField extractSchemaFieldFromSchemaMetadata(
      @Nonnull final SchemaMetadata schemaMetadata, @Nonnull final String fieldPath) {
    return schemaMetadata.getFields().stream()
        .filter(field -> fieldPath.equals(field.getFieldPath()))
        .findFirst()
        .orElse(null);
  }

  @Nullable
  private EditableSchemaMetadata getEditableSchemaMetadata(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getEditableSchemaMetadataEntityResponse(opContext, entityUrn);
    if (response != null
        && response.getAspects().containsKey(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)) {
      return new EditableSchemaMetadata(
          response.getAspects().get(EDITABLE_SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  @Nullable
  private EntityResponse getEditableSchemaMetadataEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(EDITABLE_SCHEMA_METADATA_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve editable schema metadata for entity with urn %s", entityUrn),
          e);
    }
  }

  @Nullable
  private SchemaMetadata getSchemaMetadata(
      @Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    final EntityResponse response = getSchemaMetadataEntityResponse(opContext, entityUrn);
    if (response != null && response.getAspects().containsKey(SCHEMA_METADATA_ASPECT_NAME)) {
      return new SchemaMetadata(
          response.getAspects().get(SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  @Nullable
  private EntityResponse getSchemaMetadataEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    try {
      return this.entityClient.getV2(
          opContext,
          entityUrn.getEntityType(),
          entityUrn,
          ImmutableSet.of(SCHEMA_METADATA_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve schema metadata for entity with urn %s", entityUrn), e);
    }
  }

  @Nonnull
  protected static Map<Urn, EditableSchemaMetadata> getEditableSchemaMetadataAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> entityUrns,
      @Nonnull EditableSchemaMetadata defaultValue) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyMap();
    }

    try {
      Map<Urn, Aspect> aspects =
          batchGetLatestAspect(
              opContext, entityUrns, Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);

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
          batchGetLatestAspect(opContext, entityUrns, Constants.OWNERSHIP_ASPECT_NAME);

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
          batchGetLatestAspect(opContext, entityUrns, Constants.GLOSSARY_TERMS_ASPECT_NAME);

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
          batchGetLatestAspect(opContext, entityUrns, Constants.DOMAINS_ASPECT_NAME);

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
