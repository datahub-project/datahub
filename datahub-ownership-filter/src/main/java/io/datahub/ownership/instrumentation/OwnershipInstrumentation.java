package io.datahub.ownership.instrumentation;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListRecommendationsResult;
import com.linkedin.datahub.graphql.generated.RecommendationContent;
import com.linkedin.datahub.graphql.generated.RecommendationModule;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingEnvironmentImpl;
import io.datahub.ownership.access.DomainPlatformAccessResolver;
import io.datahub.ownership.admin.AdminBypass;
import io.datahub.ownership.filter.OwnershipFilterBuilder;
import io.datahub.ownership.group.CachedGroupResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class OwnershipInstrumentation extends SimplePerformantInstrumentation {

    private static final Logger log = LoggerFactory.getLogger(OwnershipInstrumentation.class);

    private static final String LIST_RECOMMENDATIONS = "listRecommendations";

    /**
     * OAuth/OIDC flows can authenticate the caller under a synthetic actor URN (derived from the
     * token issuer + subject) rather than the real DataHub user, e.g.
     * {@code urn:li:corpuser:__oauth_https___auth_example_com_alice}. Ownership/group data is stored
     * against the REAL user URN ({@code urn:li:corpuser:alice}), so filtering by the synthetic actor
     * matches nothing. When the actor is synthetic we resolve the real user URN from the
     * {@link #RESOLVED_USER_URN_CLAIM} claim that the upstream auth layer attaches.
     */
    private static final String OAUTH_SYNTHETIC_ACTOR_PREFIX = "urn:li:corpuser:__oauth_";

    private static final String RESOLVED_USER_URN_CLAIM = "__datahub_user_urn";

    /**
     * JWT claim holding the caller's group names. External services that call the API with a raw
     * JWT never go through interactive OIDC login, so DataHub has no {@code groupMembership} aspect
     * for them and group-owned assets would be invisible. We read the groups straight from the token
     * and map each {@code name -> urn:li:corpGroup:<URLEncoder.encode(name)>} — the same encoding
     * DataHub's OidcCallbackLogic uses when provisioning groups — then union them with any
     * DataHub-recorded membership.
     */
    private static final String JWT_GROUPS_CLAIM = "groups";

    /** Navigational types that are visible to everyone, always (not ownership- or access-gated). */
    private static final Set<String> EXEMPT_TYPES = Set.of("GLOSSARY_TERM", "GLOSSARY_NODE", "TAG");

    private static final Set<String> DOMAIN_TYPES = Set.of("DOMAIN");
    private static final Set<String> PLATFORM_TYPES = Set.of("DATA_PLATFORM", "DATA_PLATFORM_INSTANCE");

    /**
     * Domain/platform types: not ownership-gated on their own (they have no owners), but ACCESS-gated
     * — an actor only sees a domain/platform that contains at least one asset they can view.
     */
    private static final Set<String> ACCESS_SCOPED_TYPES;

    static {
        Set<String> s = new HashSet<>(DOMAIN_TYPES);
        s.addAll(PLATFORM_TYPES);
        ACCESS_SCOPED_TYPES = Set.copyOf(s);
    }

    /** Never-matching sentinel so an empty accessible set yields zero results (not "all"). */
    private static final String NO_MATCH_URN = "urn:li:domain:__ownership_filter_no_match__";

    private final OwnershipFilterBuilder filterBuilder;
    private final FieldArgumentMutators mutators;
    private final CachedGroupResolver groupResolver;
    private final AdminBypass adminBypass;
    private final DomainPlatformAccessResolver accessResolver;
    /**
     * When false (default), the home-page {@code listRecommendations} domain/platform cards are left
     * UNFILTERED — i.e. stock DataHub behavior. Asset search filtering (the security boundary) is
     * unaffected by this flag. Enable only if you want the home-page Domains/Platforms modules
     * scoped to what the actor can access.
     */
    private final boolean recommendationsFilterEnabled;

    public OwnershipInstrumentation(
            @Nonnull OwnershipFilterBuilder filterBuilder,
            @Nonnull FieldArgumentMutators mutators,
            @Nonnull CachedGroupResolver groupResolver,
            @Nonnull AdminBypass adminBypass,
            @Nonnull DomainPlatformAccessResolver accessResolver,
            boolean recommendationsFilterEnabled) {
        this.filterBuilder = filterBuilder;
        this.mutators = mutators;
        this.groupResolver = groupResolver;
        this.adminBypass = adminBypass;
        this.accessResolver = accessResolver;
        this.recommendationsFilterEnabled = recommendationsFilterEnabled;
    }

    @Override
    public DataFetcher<?> instrumentDataFetcher(
            DataFetcher<?> dataFetcher,
            InstrumentationFieldFetchParameters parameters,
            @Nullable InstrumentationState state) {
        String fieldName = parameters.getExecutionStepInfo().getFieldDefinition().getName();
        // listRecommendations is only intercepted when recommendation filtering is explicitly
        // enabled; otherwise the home page renders stock (unfiltered) domains/platforms.
        boolean gated = mutators.isOwnershipGated(fieldName)
                || (recommendationsFilterEnabled && LIST_RECOMMENDATIONS.equals(fieldName));
        if (!gated) {
            return dataFetcher;
        }
        return env -> intercept(dataFetcher, env, fieldName);
    }

    @SuppressWarnings("deprecation")
    private Object intercept(DataFetcher<?> original, DataFetchingEnvironment env, String fieldName)
            throws Exception {
        // QueryContext placement varies by DataHub version: newer builds put it in the GraphQL
        // context map; v1.3.x attaches it as the (legacy) DataFetchingEnvironment context. Read both.
        QueryContext qc = env.getGraphQlContext().get(QueryContext.class);
        if (qc == null && env.getContext() instanceof QueryContext legacyCtx) {
            qc = legacyCtx;
        }
        if (qc == null) {
            log.warn("OwnershipInstrumentation: no QueryContext on field {}; passing through", fieldName);
            return original.get(env);
        }

        // Resolve the REAL user behind any synthetic OAuth actor, so ownership/group matching uses
        // the same identity that owns the data. All downstream checks (groups, filter injection,
        // domain/platform scoping) run as this effective actor.
        Urn actor = resolveEffectiveActor(qc);
        // Intentionally not caught: fail-closed. A transient group-service outage surfaces as
        // a GraphQL error rather than silently granting unfiltered access.
        List<Urn> groups = new ArrayList<>(groupResolver.groupsFor(qc.getOperationContext(), actor));
        // Augment with groups asserted by the JWT, so a raw-JWT API client (never provisioned via
        // interactive login) is still matched against its group-owned assets.
        for (Urn jwtGroup : groupsFromJwt(qc)) {
            if (!groups.contains(jwtGroup)) {
                groups.add(jwtGroup);
            }
        }

        if (adminBypass.isAdmin(actor, groups)) {
            return original.get(env);
        }

        // Home-page recommendation cards: filter out domain/platform cards the actor can't access.
        // This is best-effort navigation, NOT the security boundary (assets are filtered in search),
        // so it must never break the whole home page: if the access computation or filtering fails,
        // fall back to the unfiltered recommendations.
        if (LIST_RECOMMENDATIONS.equals(fieldName)) {
            DomainPlatformAccessResolver.AccessSets access;
            try {
                access = accessResolver.resolve(qc.getOperationContext(), actor, groups);
            } catch (RuntimeException e) {
                log.warn("OwnershipInstrumentation: domain/platform access resolution failed for "
                        + "listRecommendations (actor={}); returning unfiltered recommendations", actor, e);
                return original.get(env);
            }
            Object result = original.get(env);
            if (result instanceof CompletableFuture<?> future) {
                return future.thenApply(r -> safeFilterRecommendations(r, access));
            }
            return safeFilterRecommendations(result, access);
        }

        Object inputObj = env.getArgument("input");
        if (!(inputObj instanceof Map)) {
            log.warn("OwnershipInstrumentation: field {} has no Map 'input' arg; passing through", fieldName);
            return original.get(env);
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> input = new LinkedHashMap<>((Map<String, Object>) inputObj);

        List<String> types = requestedTypes(input);

        // Glossary/tags: always visible.
        if (!types.isEmpty() && EXEMPT_TYPES.containsAll(types)) {
            return original.get(env);
        }

        if (!types.isEmpty() && ACCESS_SCOPED_TYPES.containsAll(types)) {
            // Domain/platform search: restrict to the URNs the actor can access.
            DomainPlatformAccessResolver.AccessSets access =
                    accessResolver.resolve(qc.getOperationContext(), actor, groups);
            Set<String> allowed = new HashSet<>();
            if (types.stream().anyMatch(DOMAIN_TYPES::contains)) {
                allowed.addAll(access.domains());
            }
            if (types.stream().anyMatch(PLATFORM_TYPES::contains)) {
                allowed.addAll(access.platforms());
            }
            mutators.applyOwnershipFilter(fieldName, input, urnPredicate(allowed));
        } else {
            // Assets (or unscoped queries): inject the owners-or-unowned predicate.
            List<String> groupStrings = groups.stream().map(Urn::toString).toList();
            mutators.applyOwnershipFilter(fieldName, input,
                    filterBuilder.injectOwnershipFilter(actor.toString(), groupStrings));
        }

        Map<String, Object> newArgs = new LinkedHashMap<>(env.getArguments());
        newArgs.put("input", input);

        DataFetchingEnvironment mutatedEnv = DataFetchingEnvironmentImpl
                .newDataFetchingEnvironment(env)
                .arguments(newArgs)
                .build();

        return original.get(mutatedEnv);
    }

    /**
     * Returns the effective actor: the real user URN when the authenticated actor is a synthetic
     * OAuth principal AND the upstream auth layer supplied a {@link #RESOLVED_USER_URN_CLAIM} claim;
     * otherwise the authenticated actor URN as-is. Ownership/group data is keyed on the real user,
     * so this keeps filtering consistent with the authenticated identity. It does not validate the
     * token (that is the authenticator's job) — it only maps the already-authenticated identity.
     */
    private Urn resolveEffectiveActor(@Nonnull QueryContext qc) throws Exception {
        String actorUrn = qc.getActorUrn();
        if (actorUrn != null && actorUrn.startsWith(OAUTH_SYNTHETIC_ACTOR_PREFIX)) {
            try {
                Authentication auth = qc.getAuthentication();
                Map<String, Object> claims = auth == null ? null : auth.getClaims();
                Object resolved = claims == null ? null : claims.get(RESOLVED_USER_URN_CLAIM);
                if (resolved != null && !resolved.toString().isBlank()) {
                    return Urn.createFromString(resolved.toString());
                }
            } catch (RuntimeException e) {
                log.warn("OwnershipInstrumentation: could not resolve real user URN from claim '{}' "
                        + "for synthetic actor {}; using actor as-is", RESOLVED_USER_URN_CLAIM, actorUrn, e);
            }
        }
        return Urn.createFromString(actorUrn);
    }

    /**
     * Groups asserted by the caller's JWT ({@link #JWT_GROUPS_CLAIM}), mapped to corpGroup URNs the
     * same way DataHub's OIDC provisioning does. Best-effort: returns empty on any problem so a
     * malformed claim never breaks filtering (DataHub-recorded membership still applies).
     */
    private List<Urn> groupsFromJwt(@Nonnull QueryContext qc) {
        try {
            Authentication auth = qc.getAuthentication();
            Map<String, Object> claims = auth == null ? null : auth.getClaims();
            Object raw = claims == null ? null : claims.get(JWT_GROUPS_CLAIM);
            if (raw == null) {
                return List.of();
            }
            Collection<?> names = raw instanceof Collection<?> col
                    ? col
                    : List.of(raw.toString().split(","));
            List<Urn> result = new ArrayList<>();
            for (Object n : names) {
                String name = n == null ? null : n.toString().trim();
                if (name == null || name.isEmpty()) {
                    continue;
                }
                String encoded = URLEncoder.encode(name, StandardCharsets.UTF_8);
                result.add(Urn.createFromString("urn:li:corpGroup:" + encoded));
            }
            return result;
        } catch (Exception e) {
            log.warn("OwnershipInstrumentation: failed to derive groups from JWT claim '{}'; "
                    + "using DataHub-recorded membership only", JWT_GROUPS_CLAIM, e);
            return List.of();
        }
    }

    /** {@link #filterRecommendations} guarded so a failure never blanks the home page. */
    private Object safeFilterRecommendations(Object result, DomainPlatformAccessResolver.AccessSets access) {
        try {
            return filterRecommendations(result, access);
        } catch (RuntimeException e) {
            log.warn("OwnershipInstrumentation: recommendation filtering failed; "
                    + "returning unfiltered recommendations", e);
            return result;
        }
    }

    /** Single-disjunct predicate: {@code urn IN [allowed]} (sentinel when empty so nothing matches). */
    private List<Map<String, Object>> urnPredicate(Set<String> allowedUrns) {
        List<String> values = allowedUrns.isEmpty() ? List.of(NO_MATCH_URN) : new ArrayList<>(allowedUrns);
        Map<String, Object> crit = new LinkedHashMap<>();
        crit.put("field", "urn");
        crit.put("condition", "EQUAL");
        crit.put("values", values);
        Map<String, Object> disjunct = new LinkedHashMap<>();
        disjunct.put("and", new ArrayList<>(List.of(crit)));
        return new ArrayList<>(List.of(disjunct));
    }

    @Nullable
    private Object filterRecommendations(Object result, DomainPlatformAccessResolver.AccessSets access) {
        if (!(result instanceof ListRecommendationsResult recs) || recs.getModules() == null) {
            return result;
        }
        for (RecommendationModule module : recs.getModules()) {
            List<RecommendationContent> content = module.getContent();
            if (content == null) {
                continue;
            }
            List<RecommendationContent> kept = content.stream()
                    .filter(c -> isVisible(c.getEntity(), access))
                    .collect(Collectors.toList());
            module.setContent(kept);
        }
        return recs;
    }

    /** Domain/platform recommendation entities are kept only if accessible; other entities pass. */
    private boolean isVisible(@Nullable Entity entity, DomainPlatformAccessResolver.AccessSets access) {
        if (entity == null || entity.getType() == null) {
            return true;
        }
        EntityType type = entity.getType();
        if (type == EntityType.DOMAIN) {
            return access.domains().contains(entity.getUrn());
        }
        if (type == EntityType.DATA_PLATFORM || type == EntityType.DATA_PLATFORM_INSTANCE) {
            return access.platforms().contains(entity.getUrn());
        }
        return true;
    }

    /** Reads {@code types} (list) and {@code type} (single) entity-type args from the input. */
    private List<String> requestedTypes(@Nonnull Map<String, Object> input) {
        List<String> types = new ArrayList<>();
        Object typesObj = input.get("types");
        if (typesObj instanceof List<?> list) {
            for (Object t : list) {
                if (t != null) types.add(String.valueOf(t));
            }
        }
        Object typeObj = input.get("type");
        if (typeObj != null) {
            types.add(String.valueOf(typeObj));
        }
        return types;
    }
}
