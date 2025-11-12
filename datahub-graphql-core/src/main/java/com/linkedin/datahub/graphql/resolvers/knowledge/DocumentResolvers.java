package com.linkedin.datahub.graphql.resolvers.knowledge;

import com.linkedin.datahub.graphql.resolvers.load.EntityRelationshipsResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.datahub.graphql.types.knowledge.DocumentType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.DocumentService;
import com.linkedin.metadata.timeline.TimelineService;
import graphql.schema.idl.RuntimeWiring;
import javax.annotation.Nonnull;

/** Configures resolvers for Document query, mutation, and type wiring. */
public class DocumentResolvers {

  private static final String QUERY_TYPE = "Query";
  private static final String MUTATION_TYPE = "Mutation";

  private final DocumentService documentService;
  private final java.util.List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> entityTypes;
  private final DocumentType documentType;
  private final EntityClient entityClient;
  private final EntityService entityService;
  private final com.linkedin.metadata.graph.GraphClient graphClient;
  private final EntityRegistry entityRegistry;
  private final TimelineService timelineService;

  public DocumentResolvers(
      @Nonnull DocumentService documentService,
      @Nonnull java.util.List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> entityTypes,
      @Nonnull DocumentType documentType,
      @Nonnull EntityClient entityClient,
      @Nonnull EntityService entityService,
      @Nonnull com.linkedin.metadata.graph.GraphClient graphClient,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull TimelineService timelineService) {
    this.documentService = documentService;
    this.entityTypes = entityTypes;
    this.documentType = documentType;
    this.entityClient = entityClient;
    this.entityService = entityService;
    this.graphClient = graphClient;
    this.entityRegistry = entityRegistry;
    this.timelineService = timelineService;
  }

  public void configureResolvers(final RuntimeWiring.Builder builder) {
    // Query resolvers
    builder.type(
        QUERY_TYPE,
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "document",
                    new com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver<>(
                        documentType, (env) -> env.getArgument("urn")))
                .dataFetcher(
                    "searchDocuments",
                    new com.linkedin.datahub.graphql.resolvers.knowledge.SearchDocumentsResolver(
                        documentService, entityClient)));

    // Mutation resolvers
    builder.type(
        MUTATION_TYPE,
        typeWiring ->
            typeWiring
                .dataFetcher(
                    "createDocument",
                    new com.linkedin.datahub.graphql.resolvers.knowledge.CreateDocumentResolver(
                        documentService, entityService))
                .dataFetcher(
                    "updateDocumentContents",
                    new com.linkedin.datahub.graphql.resolvers.knowledge
                        .UpdateDocumentContentsResolver(documentService))
                .dataFetcher(
                    "updateDocumentRelatedEntities",
                    new com.linkedin.datahub.graphql.resolvers.knowledge
                        .UpdateDocumentRelatedEntitiesResolver(documentService))
                .dataFetcher(
                    "moveDocument",
                    new com.linkedin.datahub.graphql.resolvers.knowledge.MoveDocumentResolver(
                        documentService))
                .dataFetcher(
                    "deleteDocument",
                    new com.linkedin.datahub.graphql.resolvers.knowledge.DeleteDocumentResolver(
                        documentService))
                .dataFetcher(
                    "updateDocumentStatus",
                    new com.linkedin.datahub.graphql.resolvers.knowledge
                        .UpdateDocumentStatusResolver(documentService))
                .dataFetcher(
                    "updateDocumentSubType",
                    new com.linkedin.datahub.graphql.resolvers.knowledge
                        .UpdateDocumentSubTypeResolver(documentService))
                .dataFetcher(
                    "mergeDraft",
                    new com.linkedin.datahub.graphql.resolvers.knowledge.MergeDraftResolver(
                        documentService, entityService)));

    // Type wiring for Document root
    builder.type(
        "Document",
        typeWiring ->
            typeWiring
                .dataFetcher("relationships", new EntityRelationshipsResultResolver(graphClient))
                .dataFetcher(
                    "aspects",
                    new com.linkedin.datahub.graphql.WeaklyTypedAspectsResolver(
                        entityClient, entityRegistry))
                .dataFetcher(
                    "privileges",
                    new com.linkedin.datahub.graphql.resolvers.entity.EntityPrivilegesResolver(
                        entityClient))
                .dataFetcher(
                    "drafts",
                    new com.linkedin.datahub.graphql.resolvers.knowledge.DocumentDraftsResolver(
                        documentService))
                .dataFetcher(
                    "changeHistory",
                    new com.linkedin.datahub.graphql.resolvers.knowledge
                        .DocumentChangeHistoryResolver(timelineService))
                .dataFetcher(
                    "parentDocuments",
                    new com.linkedin.datahub.graphql.resolvers.knowledge.ParentDocumentsResolver(
                        entityClient)));

    // Resolve DocumentInfo.relatedAssets[].asset -> Entity (resolved)
    builder.type(
        "DocumentRelatedAsset",
        typeWiring ->
            typeWiring.dataFetcher(
                "asset",
                new EntityTypeResolver(
                    entityTypes,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.DocumentRelatedAsset)
                                env.getSource())
                            .getAsset())));

    // Resolve DocumentInfo.relatedArticles[].document -> Document (resolved)
    builder.type(
        "DocumentRelatedDocument",
        typeWiring ->
            typeWiring.dataFetcher(
                "document",
                new LoadableTypeResolver<>(
                    documentType,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.DocumentRelatedDocument)
                                env.getSource())
                            .getDocument()
                            .getUrn())));

    // Resolve DocumentInfo.parentArticle.document -> Document (resolved)
    builder.type(
        "DocumentParentDocument",
        typeWiring ->
            typeWiring.dataFetcher(
                "document",
                new LoadableTypeResolver<>(
                    documentType,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.DocumentParentDocument)
                                env.getSource())
                            .getDocument()
                            .getUrn())));

    // Resolve DocumentInfo.draftOf.document -> Document (resolved)
    builder.type(
        "DocumentDraftOf",
        typeWiring ->
            typeWiring.dataFetcher(
                "document",
                new LoadableTypeResolver<>(
                    documentType,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.DocumentDraftOf) env.getSource())
                            .getDocument()
                            .getUrn())));

    // Resolve DocumentChange.actor -> CorpUser (resolved)
    builder.type(
        "DocumentChange",
        typeWiring ->
            typeWiring.dataFetcher(
                "actor",
                new EntityTypeResolver(
                    entityTypes,
                    (env) ->
                        ((com.linkedin.datahub.graphql.generated.DocumentChange) env.getSource())
                            .getActor())));
  }
}
