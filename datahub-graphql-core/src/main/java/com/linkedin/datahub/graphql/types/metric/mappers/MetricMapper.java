package com.linkedin.datahub.graphql.types.metric.mappers;

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
import com.linkedin.datahub.graphql.generated.EntityEdge;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Metric;
import com.linkedin.datahub.graphql.generated.MetricInfo;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DocumentationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.EdgeMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.SubTypesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.MetricKey;
import com.linkedin.structured.StructuredProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class MetricMapper {

  private MetricMapper() {}

  public static Metric map(@Nullable QueryContext context, final EntityResponse entityResponse) {
    final Metric result = new Metric();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.METRIC);

    final EnvelopedAspect envelopedKey = aspects.get(Constants.METRIC_KEY_ASPECT_NAME);
    if (envelopedKey != null) {
      final MetricKey key = new MetricKey(envelopedKey.getValue().data());
      result.setPlatform(
          DataPlatform.builder()
              .setType(EntityType.DATA_PLATFORM)
              .setUrn(key.getPlatform().toString())
              .build());
      result.setPath(key.getPath());
      result.setId(key.getId());
    }

    final EnvelopedAspect envelopedInfo = aspects.get(Constants.METRIC_INFO_ASPECT_NAME);
    if (envelopedInfo != null) {
      final com.linkedin.metric.MetricInfo pdlInfo =
          new com.linkedin.metric.MetricInfo(envelopedInfo.getValue().data());
      result.setInfo(mapMetricInfo(pdlInfo));
      if (pdlInfo.hasSemanticModel() && pdlInfo.getSemanticModel() != null) {
        final SemanticModel semanticModelStub = new SemanticModel();
        semanticModelStub.setUrn(pdlInfo.getSemanticModel().toString());
        semanticModelStub.setType(EntityType.SEMANTIC_MODEL);
        result.setSemanticModel(semanticModelStub);
      }
    }

    final EnvelopedAspect envelopedRelationships =
        aspects.get(Constants.METRIC_RELATIONSHIPS_ASPECT_NAME);
    if (envelopedRelationships != null) {
      result.setMetricRelationships(
          mapMetricRelationships(
              context,
              new com.linkedin.metric.MetricRelationships(
                  envelopedRelationships.getValue().data())));
    }

    final EnvelopedAspect envelopedUpstreams = aspects.get(Constants.METRIC_UPSTREAMS_ASPECT_NAME);
    if (envelopedUpstreams != null) {
      result.setMetricUpstreams(
          MetricUpstreamsMapper.map(
              context,
              new com.linkedin.metric.MetricUpstreams(envelopedUpstreams.getValue().data())));
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
          result, Metric.class);
    }
    return result;
  }

  private static MetricInfo mapMetricInfo(final com.linkedin.metric.MetricInfo pdl) {
    final MetricInfo result = new MetricInfo();
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
    if (pdl.hasExpression() && pdl.getExpression() != null) {
      result.setExpression(MetricExpressionMapper.map(pdl.getExpression()));
    }
    if (pdl.hasAiContext() && pdl.getAiContext() != null) {
      result.setAiContext(AiContextMapper.map(pdl.getAiContext()));
    }
    return result;
  }

  private static com.linkedin.datahub.graphql.generated.MetricRelationships mapMetricRelationships(
      @Nullable QueryContext context, final com.linkedin.metric.MetricRelationships pdl) {
    final com.linkedin.datahub.graphql.generated.MetricRelationships result =
        new com.linkedin.datahub.graphql.generated.MetricRelationships();

    if (pdl.hasParentMetric() && pdl.getParentMetric() != null) {
      result.setParentMetric((Metric) UrnToEntityMapper.map(context, pdl.getParentMetric()));
    }

    final List<EntityEdge> derivedFrom;
    if (pdl.hasDerivedFrom() && pdl.getDerivedFrom() != null) {
      derivedFrom =
          EdgeMapper.mapList(
              context,
              pdl.getDerivedFrom().stream()
                  .map(d -> new com.linkedin.common.Edge(d.data()))
                  .collect(Collectors.toList()));
    } else {
      derivedFrom = Collections.emptyList();
    }
    result.setDerivedFrom(derivedFrom);

    final List<EntityEdge> relatedMetrics;
    if (pdl.hasRelatedMetrics() && pdl.getRelatedMetrics() != null) {
      relatedMetrics = EdgeMapper.mapList(context, new ArrayList<>(pdl.getRelatedMetrics()));
    } else {
      relatedMetrics = Collections.emptyList();
    }
    result.setRelatedMetrics(relatedMetrics);

    return result;
  }
}
