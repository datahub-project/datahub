package com.linkedin.datahub.graphql.types.semanticmodel.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Documentation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipCardinality;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ModelDataset;
import com.linkedin.datahub.graphql.generated.SemanticField;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.datahub.graphql.generated.SemanticModelInfo;
import com.linkedin.datahub.graphql.generated.SemanticModelRelationship;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DocumentationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.FineGrainedLineagesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.SubTypesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.SemanticModelKey;
import com.linkedin.structured.StructuredProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class SemanticModelMapper {

  private SemanticModelMapper() {}

  public static SemanticModel map(
      @Nullable QueryContext context, final EntityResponse entityResponse) {
    final SemanticModel result = new SemanticModel();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.SEMANTIC_MODEL);

    final EnvelopedAspect envelopedKey = aspects.get(Constants.SEMANTIC_MODEL_KEY_ASPECT_NAME);
    if (envelopedKey != null) {
      final SemanticModelKey key = new SemanticModelKey(envelopedKey.getValue().data());
      result.setPlatform(
          DataPlatform.builder()
              .setType(EntityType.DATA_PLATFORM)
              .setUrn(key.getPlatform().toString())
              .build());
      result.setPath(key.getPath());
      result.setId(key.getId());
    }

    final EnvelopedAspect envelopedInfo = aspects.get(Constants.SEMANTIC_MODEL_INFO_ASPECT_NAME);
    if (envelopedInfo != null) {
      final com.linkedin.semanticmodel.SemanticModelInfo pdlInfo =
          new com.linkedin.semanticmodel.SemanticModelInfo(envelopedInfo.getValue().data());
      result.setInfo(mapSemanticModelInfo(context, pdlInfo, entityUrn));
    }

    final EnvelopedAspect envelopedUpstreamLineage =
        aspects.get(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    if (envelopedUpstreamLineage != null) {
      final UpstreamLineage upstreamLineage =
          new UpstreamLineage(envelopedUpstreamLineage.getValue().data());
      if (upstreamLineage.hasFineGrainedLineages()
          && upstreamLineage.getFineGrainedLineages() != null) {
        result.setFineGrainedLineages(
            FineGrainedLineagesMapper.map(upstreamLineage.getFineGrainedLineages()));
      }
    }

    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedGlobalTags = aspects.get(Constants.GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedGlobalTags != null) {
      result.setTags(
          GlobalTagsMapper.map(
              context, new GlobalTags(envelopedGlobalTags.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedGlossaryTerms =
        aspects.get(Constants.GLOSSARY_TERMS_ASPECT_NAME);
    if (envelopedGlossaryTerms != null) {
      result.setGlossaryTerms(
          GlossaryTermsMapper.map(
              context, new GlossaryTerms(envelopedGlossaryTerms.getValue().data()), entityUrn));
    }

    final EnvelopedAspect envelopedDomains = aspects.get(Constants.DOMAINS_ASPECT_NAME);
    if (envelopedDomains != null) {
      final Domains domains = new Domains(envelopedDomains.getValue().data());
      if (domains.hasDomains() && !domains.getDomains().isEmpty()) {
        result.setDomain(DomainAssociationMapper.map(context, domains, entityUrn.toString()));
      }
    }

    final EnvelopedAspect envelopedInstitutionalMemory =
        aspects.get(Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME);
    if (envelopedInstitutionalMemory != null) {
      result.setInstitutionalMemory(
          InstitutionalMemoryMapper.map(
              context,
              new InstitutionalMemory(envelopedInstitutionalMemory.getValue().data()),
              entityUrn));
    }

    final EnvelopedAspect envelopedStructuredProps =
        aspects.get(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME);
    if (envelopedStructuredProps != null) {
      result.setStructuredProperties(
          StructuredPropertiesMapper.map(
              context,
              new StructuredProperties(envelopedStructuredProps.getValue().data()),
              entityUrn));
    }

    final EnvelopedAspect envelopedStatus = aspects.get(Constants.STATUS_ASPECT_NAME);
    if (envelopedStatus != null) {
      final Status status = new Status(envelopedStatus.getValue().data());
      result.setStatus(StatusMapper.map(context, status));
      result.setExists(!status.isRemoved());
    }

    final EnvelopedAspect envelopedDeprecation = aspects.get(Constants.DEPRECATION_ASPECT_NAME);
    if (envelopedDeprecation != null) {
      result.setDeprecation(
          DeprecationMapper.map(context, new Deprecation(envelopedDeprecation.getValue().data())));
    }

    final EnvelopedAspect envelopedDataPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedDataPlatformInstance != null) {
      result.setDataPlatformInstance(
          DataPlatformInstanceAspectMapper.map(
              context, new DataPlatformInstance(envelopedDataPlatformInstance.getValue().data())));
    }

    final EnvelopedAspect envelopedSubTypes = aspects.get(Constants.SUB_TYPES_ASPECT_NAME);
    if (envelopedSubTypes != null) {
      result.setSubTypes(
          SubTypesMapper.map(context, new SubTypes(envelopedSubTypes.getValue().data())));
    }

    final EnvelopedAspect envelopedDocumentation = aspects.get(Constants.DOCUMENTATION_ASPECT_NAME);
    if (envelopedDocumentation != null) {
      result.setDocumentation(
          DocumentationMapper.map(
              context, new Documentation(envelopedDocumentation.getValue().data())));
    }

    final EnvelopedAspect envelopedBrowsePathsV2 =
        aspects.get(Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    if (envelopedBrowsePathsV2 != null) {
      result.setBrowsePathV2(
          BrowsePathsV2Mapper.map(
              context, new BrowsePathsV2(envelopedBrowsePathsV2.getValue().data())));
    }

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return com.linkedin.datahub.graphql.authorization.AuthorizationUtils.restrictEntity(
          result, SemanticModel.class);
    }
    return result;
  }

  private static SemanticModelInfo mapSemanticModelInfo(
      @Nullable QueryContext context,
      final com.linkedin.semanticmodel.SemanticModelInfo pdl,
      final Urn semanticModelUrn) {
    final SemanticModelInfo result = new SemanticModelInfo();
    result.setName(pdl.getName());

    if (pdl.hasDescription() && pdl.getDescription() != null) {
      result.setDescription(pdl.getDescription());
    }
    if (pdl.hasCreated() && pdl.getCreated() != null) {
      result.setCreated(MapperUtils.createResolvedAuditStamp(pdl.getCreated()));
    }
    if (pdl.hasLastModified() && pdl.getLastModified() != null) {
      result.setLastModified(MapperUtils.createResolvedAuditStamp(pdl.getLastModified()));
    }
    if (pdl.hasNativeDefinition() && pdl.getNativeDefinition() != null) {
      result.setNativeDefinition(pdl.getNativeDefinition());
    }
    if (pdl.hasAiContext() && pdl.getAiContext() != null) {
      result.setAiContext(
          com.linkedin.datahub.graphql.types.metric.mappers.AiContextMapper.map(
              pdl.getAiContext()));
    }

    final List<ModelDataset> datasets;
    if (pdl.hasDatasets() && pdl.getDatasets() != null) {
      datasets =
          pdl.getDatasets().stream()
              .map(md -> mapModelDataset(context, md, semanticModelUrn))
              .collect(Collectors.toList());
    } else {
      datasets = Collections.emptyList();
    }
    result.setDatasets(datasets);

    if (pdl.hasRelationships() && pdl.getRelationships() != null) {
      result.setRelationships(
          pdl.getRelationships().stream()
              .map(SemanticModelMapper::mapSemanticModelRelationship)
              .collect(Collectors.toList()));
    }

    return result;
  }

  private static ModelDataset mapModelDataset(
      @Nullable QueryContext context,
      final com.linkedin.semanticmodel.ModelDataset pdl,
      final Urn semanticModelUrn) {
    final ModelDataset result = new ModelDataset();
    result.setName(pdl.getName());

    if (pdl.hasSource() && pdl.getSource() != null) {
      result.setSource(UrnToEntityMapper.map(context, pdl.getSource()));
    }

    if (pdl.hasFields() && pdl.getFields() != null) {
      final List<SemanticField> fields = new ArrayList<>();
      for (com.linkedin.semanticmodel.SemanticField field : pdl.getFields()) {
        final SemanticField mapped = SemanticFieldMapper.map(context, field, semanticModelUrn);
        if (mapped != null) {
          fields.add(mapped);
        }
      }
      result.setFields(fields);
    }

    return result;
  }

  private static SemanticModelRelationship mapSemanticModelRelationship(
      final com.linkedin.semanticmodel.SemanticModelRelationship pdl) {
    final SemanticModelRelationship result = new SemanticModelRelationship();

    if (pdl.hasName() && pdl.getName() != null) {
      result.setName(pdl.getName());
    }
    result.setFrom(pdl.getFrom());
    result.setFromColumns(new ArrayList<>(pdl.getFromColumns()));
    result.setTo(pdl.getTo());
    result.setToColumns(new ArrayList<>(pdl.getToColumns()));

    if (pdl.hasCardinality() && pdl.getCardinality() != null) {
      result.setCardinality(
          PdlEnumMapper.mapDefaultNull(ERModelRelationshipCardinality.class, pdl.getCardinality()));
    }

    if (pdl.hasAiContext() && pdl.getAiContext() != null) {
      result.setAiContext(
          com.linkedin.datahub.graphql.types.metric.mappers.AiContextMapper.map(
              pdl.getAiContext()));
    }

    return result;
  }
}
