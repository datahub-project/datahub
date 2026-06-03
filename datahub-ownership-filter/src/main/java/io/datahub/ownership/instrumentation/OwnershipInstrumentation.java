package io.datahub.ownership.instrumentation;

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
import java.util.ArrayList;
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

    public OwnershipInstrumentation(
            @Nonnull OwnershipFilterBuilder filterBuilder,
            @Nonnull FieldArgumentMutators mutators,
            @Nonnull CachedGroupResolver groupResolver,
            @Nonnull AdminBypass adminBypass,
            @Nonnull DomainPlatformAccessResolver accessResolver) {
        this.filterBuilder = filterBuilder;
        this.mutators = mutators;
        this.groupResolver = groupResolver;
        this.adminBypass = adminBypass;
        this.accessResolver = accessResolver;
    }

    @Override
    public DataFetcher<?> instrumentDataFetcher(
            DataFetcher<?> dataFetcher,
            InstrumentationFieldFetchParameters parameters,
            @Nullable InstrumentationState state) {
        String fieldName = parameters.getExecutionStepInfo().getFieldDefinition().getName();
        if (!mutators.isOwnershipGated(fieldName) && !LIST_RECOMMENDATIONS.equals(fieldName)) {
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

        Urn actor = Urn.createFromString(qc.getActorUrn());
        // Intentionally not caught: fail-closed. A transient group-service outage surfaces as
        // a GraphQL error rather than silently granting unfiltered access.
        List<Urn> groups = groupResolver.groupsFor(qc.getOperationContext(), actor);

        if (adminBypass.isAdmin(actor, groups)) {
            return original.get(env);
        }

        // Home-page recommendation cards: filter out domain/platform cards the actor can't access.
        if (LIST_RECOMMENDATIONS.equals(fieldName)) {
            DomainPlatformAccessResolver.AccessSets access =
                    accessResolver.resolve(qc.getOperationContext(), actor, groups);
            Object result = original.get(env);
            if (result instanceof CompletableFuture<?> future) {
                return future.thenApply(r -> filterRecommendations(r, access));
            }
            return filterRecommendations(result, access);
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
