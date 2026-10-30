package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.CreateGlossaryTermVersionInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.VersionSet;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.entity.versioning.VersionPropertiesInput;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.service.LifecycleStages;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.versionset.VersionSetProperties;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for the createGlossaryTermVersion mutation. Creates a new GlossaryTerm entity and links
 * it to a VersionSet so that all callers (UI, MCP, REST) share the same business logic. This takes
 * several separate writes, not one transaction; the inputs are validated before the first write so
 * expected failures leave nothing behind. When previousTermUrn is given, the new version is placed
 * under that term's parent unless parentNode is set, and an unversioned previous term becomes the
 * first version of a new set.
 */
@Slf4j
@RequiredArgsConstructor
public class CreateGlossaryTermVersionResolver
    implements DataFetcher<CompletableFuture<VersionSet>> {

  private final EntityClient _entityClient;
  private final EntityService _entityService;
  private final EntityVersioningService _entityVersioningService;
  private final FeatureFlags _featureFlags;

  /**
   * Aspects of the previous version a new version does not inherit: identity and placement (written
   * from the input), versioning and lifecycle state (set by this resolver), per-asset workflow
   * state, and values the platform computes. Every other aspect is copied, so metadata added to
   * glossary terms later is carried over too; add an aspect here if it must start fresh on each
   * version. Ownership is copied before the creator is added.
   */
  private static final Set<String> NOT_COPIED_ASPECTS =
      ImmutableSet.of(
          GLOSSARY_TERM_KEY_ASPECT_NAME,
          GLOSSARY_TERM_INFO_ASPECT_NAME,
          VERSION_PROPERTIES_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          BROWSE_PATHS_ASPECT_NAME,
          BROWSE_PATHS_V2_ASPECT_NAME,
          DEPRECATION_ASPECT_NAME,
          FORMS_ASPECT_NAME,
          TEST_RESULTS_ASPECT_NAME,
          "semanticContent",
          ORIGIN_ASPECT_NAME);

  private static final String DEFAULT_FIRST_VERSION_LABEL = "v1";

  /** The version a new version is created from. */
  private record PreviousTerm(
      Urn urn, @Nullable Urn versionSet, @Nullable Urn parentNode, EnvelopedAspectMap aspects) {}

  @Override
  public CompletableFuture<VersionSet> get(DataFetchingEnvironment environment) throws Exception {
    if (!_featureFlags.isEntityVersioning()) {
      throw new IllegalAccessError(
          "Entity Versioning is not configured, please enable before attempting to use this feature.");
    }

    final QueryContext context = environment.getContext();
    final CreateGlossaryTermVersionInput input =
        bindArgument(environment.getArgument("input"), CreateGlossaryTermVersionInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final PreviousTerm previous = fetchPreviousTerm(context, input);
            final Urn parentNode =
                input.getParentNode() != null
                    ? UrnUtils.getUrn(input.getParentNode())
                    : previous != null ? previous.parentNode() : null;

            // The previous term's version properties are rewritten too, so its node counts as well.
            final boolean previousNodeDiffers =
                previous != null && !Objects.equals(previous.parentNode(), parentNode);
            if (!GlossaryUtils.canManageChildrenEntities(context, parentNode, _entityClient)
                || (previousNodeDiffers
                    && !GlossaryUtils.canManageChildrenEntities(
                        context, previous.parentNode(), _entityClient))) {
              throw new AuthorizationException(
                  "Unauthorized to perform this action. Please contact your DataHub administrator.");
            }

            // Validate the new id before any write, so a clash cannot leave the previous term
            // linked.
            final GlossaryTermKey key = new GlossaryTermKey();
            final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
            key.setName(id);
            if (_entityClient.exists(
                context.getOperationContext(),
                EntityKeyUtils.convertEntityKeyToUrn(key, GLOSSARY_TERM_ENTITY_NAME))) {
              throw new IllegalArgumentException(
                  String.format("A GlossaryTerm with id '%s' already exists.", id));
            }

            final Urn versionSetUrn = resolveVersionSetUrn(input, previous);
            assertNoUnpublishedNewerVersion(context, versionSetUrn);
            final Urn draftStage = previous != null ? resolveDraftStage(context) : null;

            // An unversioned previous term becomes the first version of the new set.
            if (previous != null
                && previous.versionSet() == null
                && input.getVersionSet() == null) {
              _entityVersioningService.linkLatestVersion(
                  context.getOperationContext(),
                  versionSetUrn,
                  previous.urn(),
                  new VersionPropertiesInput(
                      null,
                      input.getPreviousVersionLabel() != null
                          ? input.getPreviousVersionLabel()
                          : DEFAULT_FIRST_VERSION_LABEL,
                      null,
                      null));
            }

            final String glossaryTermUrn =
                createGlossaryTerm(context, input, key, parentNode, previous);

            final Urn newTermUrn = UrnUtils.getUrn(glossaryTermUrn);
            if (draftStage != null) {
              startInDraft(context, newTermUrn, draftStage);
            }

            // Link the new term to the version set.
            final VersionPropertiesInput versionPropertiesInput =
                new VersionPropertiesInput(input.getComment(), input.getVersion(), null, null);

            final List<IngestResult> linkResults =
                _entityVersioningService.linkLatestVersion(
                    context.getOperationContext(),
                    versionSetUrn,
                    newTermUrn,
                    versionPropertiesInput);

            if (linkResults.isEmpty()) {
              throw new RuntimeException(
                  "Failed to link glossary term to version set — no ingest results returned.");
            }

            final VersionSet result = new VersionSet();
            result.setUrn(versionSetUrn.toString());
            result.setType(EntityType.VERSION_SET);
            return result;
          } catch (Exception e) {
            if (e instanceof AuthorizationException
                || e instanceof IllegalArgumentException
                || e instanceof RuntimeException) {
              throw (RuntimeException)
                  (e instanceof RuntimeException ? e : new RuntimeException(e));
            }
            log.error(
                "Failed to create GlossaryTerm version with name: {}, version: {}",
                input.getName(),
                input.getVersion(),
                e);
            throw new RuntimeException(
                String.format(
                    "Failed to create GlossaryTerm version with name: %s, version: %s",
                    input.getName(), input.getVersion()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nullable
  private PreviousTerm fetchPreviousTerm(QueryContext context, CreateGlossaryTermVersionInput input)
      throws Exception {
    if (input.getPreviousTermUrn() == null) {
      return null;
    }
    final Urn previousUrn = UrnUtils.getUrn(input.getPreviousTermUrn());
    final EntityResponse response =
        _entityClient.getV2(
            context.getOperationContext(), GLOSSARY_TERM_ENTITY_NAME, previousUrn, null);
    if (response == null || !response.getAspects().containsKey(GLOSSARY_TERM_INFO_ASPECT_NAME)) {
      throw new IllegalArgumentException(
          String.format("GlossaryTerm %s does not exist.", input.getPreviousTermUrn()));
    }
    final EnvelopedAspectMap aspects = response.getAspects();
    final GlossaryTermInfo info =
        new GlossaryTermInfo(aspects.get(GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data());
    final Urn versionSet =
        aspects.containsKey(VERSION_PROPERTIES_ASPECT_NAME)
            ? new VersionProperties(aspects.get(VERSION_PROPERTIES_ASPECT_NAME).getValue().data())
                .getVersionSet()
            : null;
    return new PreviousTerm(
        previousUrn, versionSet, info.hasParentNode() ? info.getParentNode() : null, aspects);
  }

  /** Explicit versionSet > the previous term's set > a new set. */
  private Urn resolveVersionSetUrn(
      CreateGlossaryTermVersionInput input, @Nullable PreviousTerm previous) {
    if (input.getVersionSet() != null) {
      final Urn urn = UrnUtils.getUrn(input.getVersionSet());
      if (!VERSION_SET_ENTITY_NAME.equals(urn.getEntityType())) {
        throw new IllegalArgumentException(
            String.format(
                "versionSet URN %s must be of entity type versionSet.", input.getVersionSet()));
      }
      // Checked before any write: linkLatestVersion would reject the mismatch only after the new
      // term exists, leaving it orphaned.
      final VersionSetKey key =
          (VersionSetKey)
              EntityKeyUtils.convertUrnToEntityKeyInternal(urn, new VersionSetKey().schema());
      if (!GLOSSARY_TERM_ENTITY_NAME.equals(key.getEntityType())) {
        throw new IllegalArgumentException(
            String.format(
                "versionSet %s holds %s entities, not glossary terms.",
                input.getVersionSet(), key.getEntityType()));
      }
      return urn;
    }
    if (previous != null && previous.versionSet() != null) {
      return previous.versionSet();
    }
    // Generate a new VersionSet URN. The service will create it if it does not exist.
    final VersionSetKey key = new VersionSetKey();
    key.setId(UUID.randomUUID().toString());
    key.setEntityType(GLOSSARY_TERM_ENTITY_NAME);
    return EntityKeyUtils.convertEntityKeyToUrn(key, VERSION_SET_ENTITY_NAME);
  }

  /** Creates the GlossaryTerm entity and adds the creator as owner. Returns the new URN string. */
  private String createGlossaryTerm(
      QueryContext context,
      CreateGlossaryTermVersionInput input,
      GlossaryTermKey key,
      @Nullable Urn parentNode,
      @Nullable PreviousTerm previous)
      throws Exception {
    final MetadataChangeProposal proposal =
        buildMetadataChangeProposalWithKey(
            key,
            GLOSSARY_TERM_ENTITY_NAME,
            GLOSSARY_TERM_INFO_ASPECT_NAME,
            mapTermInfo(input, parentNode));

    final String glossaryTermUrn =
        _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

    if (previous != null && !Boolean.FALSE.equals(input.getCopyMetadata())) {
      copyPreviousMetadata(context, UrnUtils.getUrn(glossaryTermUrn), previous);
    }

    OwnerUtils.addCreatorAsOwner(
        context, glossaryTermUrn, OwnerEntityType.CORP_USER, _entityService);

    return glossaryTermUrn;
  }

  private void copyPreviousMetadata(QueryContext context, Urn newTermUrn, PreviousTerm previous)
      throws Exception {
    for (Map.Entry<String, EnvelopedAspect> aspect : previous.aspects().entrySet()) {
      if (NOT_COPIED_ASPECTS.contains(aspect.getKey())) {
        continue;
      }
      _entityClient.ingestProposal(
          context.getOperationContext(),
          AspectUtils.buildMetadataChangeProposal(
              newTermUrn, aspect.getKey(), aspect.getValue().getValue()),
          false);
    }
  }

  /**
   * Rejects a new version while the set already holds a version newer than its latest, e.g. an
   * unpublished draft. The hidden draft does not become latest, so linking would derive the new
   * sortId from the latest and give two versions the same sortId.
   */
  private void assertNoUnpublishedNewerVersion(QueryContext context, Urn versionSetUrn)
      throws Exception {
    final EntityResponse versionSet =
        _entityClient.getV2(
            context.getOperationContext(),
            VERSION_SET_ENTITY_NAME,
            versionSetUrn,
            Collections.singleton(VERSION_SET_PROPERTIES_ASPECT_NAME));
    if (versionSet == null
        || !versionSet.getAspects().containsKey(VERSION_SET_PROPERTIES_ASPECT_NAME)) {
      return;
    }
    final Urn latest =
        new VersionSetProperties(
                versionSet.getAspects().get(VERSION_SET_PROPERTIES_ASPECT_NAME).getValue().data())
            .getLatest();
    // ponytail: search is eventually consistent, so a version created within the last index
    // refresh can slip past this check; a write-time guard in the versioning service would close
    // it.
    final SearchResult newest =
        _entityClient.search(
            context
                .getOperationContext()
                .withSearchFlags(
                    flags ->
                        flags
                            .setFulltext(false)
                            .setSkipCache(true)
                            .setSkipAggregates(true)
                            .setSkipHighlighting(true)
                            .setIncludeSoftDeleted(false)
                            .setFilterNonLatestVersions(false)
                            .setIncludeHiddenLifecycleStages(true)),
            GLOSSARY_TERM_ENTITY_NAME,
            "*",
            QueryUtils.newFilter(
                CriterionUtils.buildCriterion(
                    VERSION_SET_FIELD_NAME, Condition.EQUAL, versionSetUrn.toString())),
            List.of(
                new SortCriterion()
                    .setField(VERSION_SORT_ID_FIELD_NAME)
                    .setOrder(SortOrder.DESCENDING)),
            0,
            1);
    final SearchEntityArray entities = newest != null ? newest.getEntities() : null;
    if (entities != null && !entities.isEmpty() && !entities.get(0).getEntity().equals(latest)) {
      throw new IllegalArgumentException(
          String.format(
              "Version %s is newer than the latest version and not yet published. Publish or"
                  + " delete it before creating another version.",
              entities.get(0).getEntity()));
    }
  }

  /**
   * Returns the DRAFT stage a new version starts in, or null when the stage is missing or does not
   * apply to glossary terms. Fails when the stage cannot be entered from no stage, since the
   * transition hook would silently drop it and the new version would go live as latest.
   */
  @Nullable
  private Urn resolveDraftStage(QueryContext context) throws Exception {
    final Urn draftUrn = UrnUtils.getUrn(DRAFT_LIFECYCLE_STAGE_URN);
    final EntityResponse stage =
        _entityClient.getV2(
            context.getOperationContext(),
            LifecycleStages.STAGE_ENTITY_NAME,
            draftUrn,
            Collections.singleton(LifecycleStages.STAGE_INFO_ASPECT_NAME));
    if (stage == null || !stage.getAspects().containsKey(LifecycleStages.STAGE_INFO_ASPECT_NAME)) {
      log.debug("Not starting the new version in Draft: the Draft stage is not defined.");
      return null;
    }
    final LifecycleStageTypeInfo info =
        new LifecycleStageTypeInfo(
            stage.getAspects().get(LifecycleStages.STAGE_INFO_ASPECT_NAME).getValue().data());
    if (!LifecycleStages.appliesToEntityType(info, GLOSSARY_TERM_ENTITY_NAME)) {
      log.debug(
          "Not starting the new version in Draft: the stage does not apply to glossary terms.");
      return null;
    }
    if (!LifecycleStages.isTransitionAllowed(
        null, draftUrn, info.hasTransitionPolicy() ? info.getTransitionPolicy() : null)) {
      throw new IllegalArgumentException(
          String.format(
              "A new version must start in %s, but that stage's transition policy does not allow"
                  + " entering it from no stage.",
              draftUrn));
    }
    return draftUrn;
  }

  /**
   * Moves a new version to DRAFT so the previous version stays the latest visible one. Runs before
   * linking: VersionPropertiesSideEffect then declines to promote the hidden new version and the
   * previous version keeps "latest". Drafting after linking would leave LifecycleVersionSideEffect
   * to find the previous version by search, which does not see it yet within this request, so no
   * version would be latest.
   */
  private void startInDraft(QueryContext context, Urn termUrn, Urn draftUrn) throws Exception {
    final Status status =
        new Status()
            .setRemoved(false)
            .setLifecycleStage(draftUrn)
            .setLifecycleLastUpdated(
                new AuditStamp()
                    .setTime(System.currentTimeMillis())
                    .setActor(UrnUtils.getUrn(context.getActorUrn())));
    _entityClient.ingestProposal(
        context.getOperationContext(),
        AspectUtils.buildMetadataChangeProposal(termUrn, STATUS_ASPECT_NAME, status),
        false);
  }

  private GlossaryTermInfo mapTermInfo(
      CreateGlossaryTermVersionInput input, @Nullable Urn parentNode) {
    final GlossaryTermInfo info = new GlossaryTermInfo();
    info.setName(input.getName());
    info.setTermSource("INTERNAL");
    info.setDefinition(input.getDescription() != null ? input.getDescription() : "");
    if (parentNode != null) {
      try {
        info.setParentNode(
            GlossaryNodeUrn.createFromString(parentNode.toString()), SetMode.IGNORE_NULL);
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Invalid parentNode URN: %s", parentNode), e);
      }
    }
    return info;
  }
}
