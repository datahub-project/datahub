package com.linkedin.metadata.service;

import static com.linkedin.metadata.entity.AspectUtils.*;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseService {

  protected final SystemEntityClient entityClient;

  public BaseService(@Nonnull SystemEntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient);
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
      for (Urn entity : entityUrns) {
        Aspect aspect = aspects.get(entity);
        if (aspect == null) {
          finalResult.put(entity, defaultValue);
        } else {
          finalResult.put(entity, new GlobalTags(aspect.data()));
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving global tags for {} entities. Sample Urns: {} aspect: {}",
          entityUrns.size(),
          entityUrns.stream().limit(10).collect(Collectors.toList()),
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          e);
      return Collections.emptyMap();
    }
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
      for (Urn entity : entityUrns) {
        Aspect aspect = aspects.get(entity);
        if (aspect == null) {
          finalResult.put(entity, defaultValue);
        } else {
          finalResult.put(entity, new EditableSchemaMetadata(aspect.data()));
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
      for (Urn entity : entityUrns) {
        Aspect aspect = aspects.get(entity);
        if (aspect == null) {
          finalResult.put(entity, defaultValue);
        } else {
          finalResult.put(entity, new Ownership(aspect.data()));
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
      for (Urn entity : entityUrns) {
        Aspect aspect = aspects.get(entity);
        if (aspect == null) {
          finalResult.put(entity, defaultValue);
        } else {
          finalResult.put(entity, new GlossaryTerms(aspect.data()));
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
      for (Urn entity : entityUrns) {
        Aspect aspect = aspects.get(entity);
        if (aspect == null) {
          finalResult.put(entity, defaultValue);
        } else {
          finalResult.put(entity, new Domains(aspect.data()));
        }
      }
      return finalResult;
    } catch (Exception e) {
      log.error(
          "Error retrieving domains for entities. Entities: {} aspect: {}",
          entityUrns,
          Constants.DOMAIN_ENTITY_NAME,
          e);
      return Collections.emptyMap();
    }
  }

  protected void ingestChangeProposals(
      @Nonnull OperationContext opContext, @Nonnull List<MetadataChangeProposal> changes)
      throws Exception {
    // TODO: Replace this with a batch ingest proposals endpoint.
    for (MetadataChangeProposal change : changes) {
      this.entityClient.ingestProposal(opContext, change);
    }
  }
}
