package com.linkedin.datahub.graphql.types.knowledge;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.DocumentContent;
import com.linkedin.datahub.graphql.generated.DocumentDraftOf;
import com.linkedin.datahub.graphql.generated.DocumentInfo;
import com.linkedin.datahub.graphql.generated.DocumentParentDocument;
import com.linkedin.datahub.graphql.generated.DocumentRelatedAsset;
import com.linkedin.datahub.graphql.generated.DocumentRelatedDocument;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nullable;

/** Maps GMS EntityResponse representing a Document to a GraphQL Document object. */
public class DocumentMapper {

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
              new com.linkedin.knowledge.DocumentInfo(envelopedInfo.getValue().data())));
    }

    // Map SubTypes aspect to subType field (get first type if available)
    final EnvelopedAspect envelopedSubTypes = aspects.get(Constants.SUB_TYPES_ASPECT_NAME);
    if (envelopedSubTypes != null) {
      final SubTypes subTypes = new SubTypes(envelopedSubTypes.getValue().data());
      if (subTypes.hasTypeNames() && !subTypes.getTypeNames().isEmpty()) {
        result.setSubType(subTypes.getTypeNames().get(0));
      }
    }

    // Map DataPlatformInstance aspect
    final EnvelopedAspect envelopedDataPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedDataPlatformInstance != null) {
      final DataPlatformInstance dataPlatformInstance =
          new DataPlatformInstance(envelopedDataPlatformInstance.getValue().data());
      result.setDataPlatformInstance(
          DataPlatformInstanceAspectMapper.map(context, dataPlatformInstance));
    }

    // Map Ownership aspect
    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    // Map Institutional Memory aspect
    final EnvelopedAspect envelopedInstitutionalMemory =
        aspects.get(Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME);
    if (envelopedInstitutionalMemory != null) {
      result.setInstitutionalMemory(
          InstitutionalMemoryMapper.map(
              context,
              new InstitutionalMemory(envelopedInstitutionalMemory.getValue().data()),
              entityUrn));
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
  private static DocumentInfo mapDocumentInfo(final com.linkedin.knowledge.DocumentInfo info) {
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
    result.setCreated(AuditStampMapper.map(null, info.getCreated()));

    // Map lastModified audit stamp
    result.setLastModified(AuditStampMapper.map(null, info.getLastModified()));

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

    // Map draftOf - create stub that will be resolved by GraphQL batch loaders
    if (info.hasDraftOf()) {
      final DocumentDraftOf draftOfInfo = new DocumentDraftOf();
      final Document stubDraftOf = new Document();
      stubDraftOf.setUrn(info.getDraftOf().getDocument().toString());
      stubDraftOf.setType(EntityType.DOCUMENT);
      draftOfInfo.setDocument(stubDraftOf);
      result.setDraftOf(draftOfInfo);
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

    if (source.hasLastSynced()) {
      result.setLastSynced(AuditStampMapper.map(null, source.getLastSynced()));
    }

    if (source.hasProperties()) {
      result.setProperties(
          source.getProperties().entrySet().stream()
              .map(
                  entry -> {
                    final com.linkedin.datahub.graphql.generated.StringMapEntry mapEntry =
                        new com.linkedin.datahub.graphql.generated.StringMapEntry();
                    mapEntry.setKey(entry.getKey());
                    mapEntry.setValue(entry.getValue());
                    return mapEntry;
                  })
              .collect(java.util.stream.Collectors.toList()));
    }

    return result;
  }
}
