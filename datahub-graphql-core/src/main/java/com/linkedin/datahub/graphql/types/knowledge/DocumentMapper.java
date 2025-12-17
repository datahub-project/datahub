package com.linkedin.datahub.graphql.types.knowledge;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.DocumentContent;
import com.linkedin.datahub.graphql.generated.DocumentInfo;
import com.linkedin.datahub.graphql.generated.DocumentParentDocument;
import com.linkedin.datahub.graphql.generated.DocumentRelatedAsset;
import com.linkedin.datahub.graphql.generated.DocumentRelatedDocument;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
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
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nullable;

/** Maps GMS EntityResponse representing a Document to a GraphQL Document object. */
public class DocumentMapper {

  private static final String DATAHUB_DATA_PLATFORM_URN = "urn:li:dataPlatform:datahub";

  public static Document map(@Nullable QueryContext context, final EntityResponse entityResponse) {
    final Document result = new Document();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.DOCUMENT);

    // Map Document Info aspect
    final EnvelopedAspect envelopedInfo = aspects.get(Constants.DOCUMENT_INFO_ASPECT_NAME);
    if (envelopedInfo != null) {
      result.setInfo(
          mapDocumentInfo(
              new com.linkedin.knowledge.DocumentInfo(envelopedInfo.getValue().data()), entityUrn));
    }

    // Map Document Settings aspect
    final EnvelopedAspect envelopedSettings = aspects.get(Constants.DOCUMENT_SETTINGS_ASPECT_NAME);
    if (envelopedSettings != null) {
      result.setSettings(
          mapDocumentSettings(
              new com.linkedin.knowledge.DocumentSettings(envelopedSettings.getValue().data())));
    }

    // Map SubTypes aspect to subType field (get first type if available)
    final EnvelopedAspect envelopedSubTypes = aspects.get(Constants.SUB_TYPES_ASPECT_NAME);
    if (envelopedSubTypes != null) {
      final SubTypes subTypes = new SubTypes(envelopedSubTypes.getValue().data());
      if (subTypes.hasTypeNames() && !subTypes.getTypeNames().isEmpty()) {
        result.setSubType(subTypes.getTypeNames().get(0));
      }
    }

    // Map DataPlatformInstance aspect (following pattern from
    // DataJobMapper/DataProcessInstanceMapper)
    final EnvelopedAspect envelopedDataPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedDataPlatformInstance != null) {
      final DataPlatformInstance dataPlatformInstance =
          new DataPlatformInstance(envelopedDataPlatformInstance.getValue().data());
      final com.linkedin.datahub.graphql.generated.DataPlatformInstance value =
          DataPlatformInstanceAspectMapper.map(context, dataPlatformInstance);
      // Always set platform directly (resolved by platform resolver)
      result.setPlatform(value.getPlatform());
      // Only set dataPlatformInstance if there's an actual instance (to avoid null urn/type errors)
      if (dataPlatformInstance.hasInstance()) {
        result.setDataPlatformInstance(value);
      }
    } else {
      // Platform is ALWAYS required, so we set the platform to "datahub" for internal documents.
      result.setPlatform(
          com.linkedin.datahub.graphql.generated.DataPlatform.builder()
              .setType(EntityType.DATA_PLATFORM)
              .setUrn(DATAHUB_DATA_PLATFORM_URN)
              .build());
    }

    // Map Ownership aspect
    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    // Map Browse Paths V2 aspect
    final EnvelopedAspect envelopedBrowsePathsV2 =
        aspects.get(Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    if (envelopedBrowsePathsV2 != null) {
      result.setBrowsePathV2(
          BrowsePathsV2Mapper.map(
              context, new BrowsePathsV2(envelopedBrowsePathsV2.getValue().data())));
    }

    // Map Structured Properties aspect
    final EnvelopedAspect envelopedStructuredProps =
        aspects.get(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME);
    if (envelopedStructuredProps != null) {
      result.setStructuredProperties(
          StructuredPropertiesMapper.map(
              context,
              new StructuredProperties(envelopedStructuredProps.getValue().data()),
              entityUrn));
    }

    // Map Global Tags aspect
    final EnvelopedAspect envelopedGlobalTags = aspects.get(Constants.GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedGlobalTags != null) {
      result.setTags(
          GlobalTagsMapper.map(
              context, new GlobalTags(envelopedGlobalTags.getValue().data()), entityUrn));
    }

    // Map Glossary Terms aspect
    final EnvelopedAspect envelopedGlossaryTerms =
        aspects.get(Constants.GLOSSARY_TERMS_ASPECT_NAME);
    if (envelopedGlossaryTerms != null) {
      result.setGlossaryTerms(
          GlossaryTermsMapper.map(
              context, new GlossaryTerms(envelopedGlossaryTerms.getValue().data()), entityUrn));
    }

    // Map Domains aspect
    final EnvelopedAspect envelopedDomains = aspects.get(Constants.DOMAINS_ASPECT_NAME);
    if (envelopedDomains != null) {
      final Domains domains = new Domains(envelopedDomains.getValue().data());
      // domains.getDomains() returns a UrnArray
      if (domains.hasDomains() && !domains.getDomains().isEmpty()) {
        result.setDomain(DomainAssociationMapper.map(context, domains, entityUrn.toString()));
      }
    }

    // Map Status aspect for soft delete
    final EnvelopedAspect envelopedStatus = aspects.get(Constants.STATUS_ASPECT_NAME);
    if (envelopedStatus != null) {
      result.setExists(!new Status(envelopedStatus.getValue().data()).isRemoved());
    }

    // Note: Relationships are handled separately via batch resolvers in GraphQL
    // They will be resolved lazily when accessed through the GraphQL query

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return com.linkedin.datahub.graphql.authorization.AuthorizationUtils.restrictEntity(
          result, Document.class);
    } else {
      return result;
    }
  }

  /** Maps the Document Info PDL model to the GraphQL model */
  private static DocumentInfo mapDocumentInfo(
      final com.linkedin.knowledge.DocumentInfo info, final Urn entityUrn) {
    final DocumentInfo result = new DocumentInfo();

    if (info.hasTitle()) {
      result.setTitle(info.getTitle());
    }

    // Map source information if present
    if (info.hasSource()) {
      result.setSource(mapDocumentSource(info.getSource()));
    }

    // Map status
    if (info.hasStatus()) {
      result.setStatus(mapDocumentStatus(info.getStatus()));
    }

    // Map contents
    final DocumentContent graphqlContent = new DocumentContent();
    graphqlContent.setText(info.getContents().getText());
    result.setContents(graphqlContent);

    // Map created audit stamp
    result.setCreated(MapperUtils.createResolvedAuditStamp(info.getCreated()));

    // Map lastModified audit stamp
    result.setLastModified(MapperUtils.createResolvedAuditStamp(info.getLastModified()));

    // Map related assets - create stubs that will be resolved by GraphQL batch loaders
    if (info.hasRelatedAssets()) {
      result.setRelatedAssets(
          info.getRelatedAssets().stream()
              .map(
                  asset -> {
                    final DocumentRelatedAsset assetInfo = new DocumentRelatedAsset();
                    assetInfo.setAsset(
                        com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper.map(
                            null, asset.getAsset()));
                    return assetInfo;
                  })
              .collect(java.util.stream.Collectors.toList()));
    }

    // Map related documents - create stubs that will be resolved by GraphQL batch loaders
    if (info.hasRelatedDocuments()) {
      result.setRelatedDocuments(
          info.getRelatedDocuments().stream()
              .map(
                  document -> {
                    final DocumentRelatedDocument documentInfo = new DocumentRelatedDocument();
                    final Document stubDocument = new Document();
                    stubDocument.setUrn(document.getDocument().toString());
                    stubDocument.setType(EntityType.DOCUMENT);
                    documentInfo.setDocument(stubDocument);
                    return documentInfo;
                  })
              .collect(java.util.stream.Collectors.toList()));
    }

    // Map parent document - create stub that will be resolved by GraphQL batch loaders
    if (info.hasParentDocument()) {
      final DocumentParentDocument parentInfo = new DocumentParentDocument();
      final Document stubParent = new Document();
      stubParent.setUrn(info.getParentDocument().getDocument().toString());
      stubParent.setType(EntityType.DOCUMENT);
      parentInfo.setDocument(stubParent);
      result.setParentDocument(parentInfo);
    }

    // Map custom properties (included via CustomProperties mixin in PDL)
    if (info.hasCustomProperties() && !info.getCustomProperties().isEmpty()) {
      result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
    }

    return result;
  }

  /** Maps the Document Status PDL model to the GraphQL model */
  private static com.linkedin.datahub.graphql.generated.DocumentStatus mapDocumentStatus(
      final com.linkedin.knowledge.DocumentStatus status) {
    final com.linkedin.datahub.graphql.generated.DocumentStatus result =
        new com.linkedin.datahub.graphql.generated.DocumentStatus();

    // Map state
    result.setState(
        com.linkedin.datahub.graphql.generated.DocumentState.valueOf(status.getState().name()));

    return result;
  }

  /** Maps the Document Source PDL model to the GraphQL model */
  private static com.linkedin.datahub.graphql.generated.DocumentSource mapDocumentSource(
      final com.linkedin.knowledge.DocumentSource source) {
    final com.linkedin.datahub.graphql.generated.DocumentSource result =
        new com.linkedin.datahub.graphql.generated.DocumentSource();

    // Map the PDL enum to the GraphQL enum
    result.setSourceType(
        com.linkedin.datahub.graphql.generated.DocumentSourceType.valueOf(
            source.getSourceType().name()));

    if (source.hasExternalUrl()) {
      result.setExternalUrl(source.getExternalUrl());
    }

    if (source.hasExternalId()) {
      result.setExternalId(source.getExternalId());
    }

    return result;
  }

  /** Maps the Document Settings PDL model to the GraphQL model */
  private static com.linkedin.datahub.graphql.generated.DocumentSettings mapDocumentSettings(
      final com.linkedin.knowledge.DocumentSettings settings) {
    final com.linkedin.datahub.graphql.generated.DocumentSettings result =
        new com.linkedin.datahub.graphql.generated.DocumentSettings();

    result.setShowInGlobalContext(settings.isShowInGlobalContext());

    return result;
  }
}
