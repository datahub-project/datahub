package io.datahub.ownership.instrumentation;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingEnvironmentImpl;
import graphql.schema.GraphQLFieldDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahub.ownership.access.DomainPlatformAccessResolver;
import io.datahub.ownership.admin.AdminBypass;
import io.datahub.ownership.filter.OwnershipFilterBuilder;
import io.datahub.ownership.group.CachedGroupResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class OwnershipInstrumentationTest {

    private final Urn alice = urn("urn:li:corpuser:alice");
    private final Urn bob = urn("urn:li:corpuser:bob");
    private final Urn datahub = urn("urn:li:corpuser:datahub");
    private final Urn g1 = urn("urn:li:corpGroup:g1");

    private CachedGroupResolver groups;
    private AdminBypass admins;
    private DomainPlatformAccessResolver accessResolver;
    private OwnershipInstrumentation instrumentation;

    static Urn urn(String s) {
        try {
            return Urn.createFromString(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        groups = mock(CachedGroupResolver.class);
        admins = new AdminBypass(Set.of(datahub), Set.of());
        accessResolver = mock(DomainPlatformAccessResolver.class);
        when(accessResolver.resolve(any(), any(), any()))
                .thenReturn(new DomainPlatformAccessResolver.AccessSets(
                        Set.of("urn:li:domain:CBP"), Set.of("urn:li:dataPlatform:mysql")));
        instrumentation = new OwnershipInstrumentation(
                new OwnershipFilterBuilder(), new FieldArgumentMutators(), groups, admins, accessResolver, true);

        when(groups.groupsFor(any(), eq(alice))).thenReturn(List.of(g1));
        when(groups.groupsFor(any(), eq(bob))).thenReturn(List.of());
        when(groups.groupsFor(any(), eq(datahub))).thenReturn(List.of());
    }

    @Test
    void wrapsGatedFieldFetchersOnly() {
        DataFetcher<?> raw = env -> "result";
        DataFetcher<?> wrappedSearch = instrumentation.instrumentDataFetcher(raw,
                fieldFetchParams("searchAcrossEntities"), null);
        DataFetcher<?> wrappedDataset = instrumentation.instrumentDataFetcher(raw,
                fieldFetchParams("dataset"), null);

        assertThat(wrappedSearch).isNotSameAs(raw);
        assertThat(wrappedDataset).isSameAs(raw);
    }

    @Test
    void differentActorsProduceDifferentMutations() throws Exception {
        DataFetcher<?> capturingFetcher = env -> {
            Map<String, Object> input = (Map<String, Object>) env.getArgument("input");
            return new LinkedHashMap<>(input);
        };

        DataFetcher<?> wrapped = instrumentation.instrumentDataFetcher(capturingFetcher,
                fieldFetchParams("searchAcrossEntities"), null);

        Map<String, Object> aliceArgs = (Map<String, Object>) wrapped.get(envFor(alice, baseInput()));
        Map<String, Object> bobArgs = (Map<String, Object>) wrapped.get(envFor(bob, baseInput()));

        List<Map<String, Object>> aliceOr = (List<Map<String, Object>>) aliceArgs.get("orFilters");
        List<Map<String, Object>> bobOr = (List<Map<String, Object>>) bobArgs.get("orFilters");

        List<String> aliceVals = (List<String>)
                ((List<Map<String, Object>>) aliceOr.get(0).get("and")).get(0).get("values");
        List<String> bobVals = (List<String>)
                ((List<Map<String, Object>>) bobOr.get(0).get("and")).get(0).get("values");

        assertThat(aliceVals).containsExactlyInAnyOrder(alice.toString(), g1.toString());
        assertThat(bobVals).containsExactly(bob.toString());
    }

    @Test
    void oauthSyntheticActorUsesResolvedUserUrnWhenAvailable() throws Exception {
        DataFetcher<?> capturing = env ->
                new LinkedHashMap<>((Map<String, Object>) env.getArgument("input"));
        DataFetcher<?> wrapped = instrumentation.instrumentDataFetcher(capturing,
                fieldFetchParams("searchAcrossEntities"), null);

        // Authenticated as a synthetic OAuth actor, but the auth layer carries the real user URN.
        String synthetic = "urn:li:corpuser:__oauth_https___auth_example_com_alice";
        Map<String, Object> claims = Map.of("__datahub_user_urn", alice.toString());

        Map<String, Object> args =
                (Map<String, Object>) wrapped.get(envForClaims(synthetic, claims, baseInput()));

        List<Map<String, Object>> orFilters = (List<Map<String, Object>>) args.get("orFilters");
        List<String> vals = (List<String>)
                ((List<Map<String, Object>>) orFilters.get(0).get("and")).get(0).get("values");

        // Ownership predicate is built for the REAL user (alice) + her groups (g1), never the
        // synthetic OAuth actor.
        assertThat(vals).containsExactlyInAnyOrder(alice.toString(), g1.toString());
        assertThat(vals).doesNotContain(synthetic);
    }

    @Test
    void jwtGroupsClaimAugmentsOwnershipFilterForUnprovisionedUser() throws Exception {
        DataFetcher<?> capturing = env ->
                new LinkedHashMap<>((Map<String, Object>) env.getArgument("input"));
        DataFetcher<?> wrapped = instrumentation.instrumentDataFetcher(capturing,
                fieldFetchParams("searchAcrossEntities"), null);

        // bob has NO DataHub group membership (groupsFor(bob) -> []), mimicking an external service
        // that never logged in via OIDC. Its JWT asserts membership in CBP and DHS.
        Map<String, Object> claims = Map.of("groups", List.of("CBP", "DHS"));

        Map<String, Object> args =
                (Map<String, Object>) wrapped.get(envForClaims(bob.toString(), claims, baseInput()));

        List<String> vals = (List<String>)
                ((List<Map<String, Object>>) ((List<Map<String, Object>>) args.get("orFilters"))
                        .get(0).get("and")).get(0).get("values");

        // The JWT-asserted groups are honored even without provisioned membership.
        assertThat(vals).contains(bob.toString(),
                "urn:li:corpGroup:CBP", "urn:li:corpGroup:DHS");
    }

    @Test
    void listRecommendationsWrappedOnlyWhenRecommendationFilteringEnabled() {
        DataFetcher<?> raw = env -> "x";
        OwnershipInstrumentation off = new OwnershipInstrumentation(
                new OwnershipFilterBuilder(), new FieldArgumentMutators(), groups, admins, accessResolver, false);
        OwnershipInstrumentation on = new OwnershipInstrumentation(
                new OwnershipFilterBuilder(), new FieldArgumentMutators(), groups, admins, accessResolver, true);

        // Disabled (default): listRecommendations is NOT wrapped -> stock, unfiltered home page.
        assertThat(off.instrumentDataFetcher(raw, fieldFetchParams("listRecommendations"), null)).isSameAs(raw);
        // Enabled: it IS wrapped so domain/platform cards get scoped.
        assertThat(on.instrumentDataFetcher(raw, fieldFetchParams("listRecommendations"), null)).isNotSameAs(raw);
        // Asset search stays gated regardless of the flag (the real security boundary).
        assertThat(off.instrumentDataFetcher(raw, fieldFetchParams("searchAcrossEntities"), null)).isNotSameAs(raw);
    }

    @Test
    void adminBypassesFilterInjection() throws Exception {
        AtomicReference<Map<String, Object>> seen = new AtomicReference<>();
        DataFetcher<?> capturing = env -> {
            seen.set(new LinkedHashMap<>((Map<String, Object>) env.getArgument("input")));
            return null;
        };

        DataFetcher<?> wrapped = instrumentation.instrumentDataFetcher(capturing,
                fieldFetchParams("searchAcrossEntities"), null);

        wrapped.get(envFor(datahub, baseInput()));

        assertThat(seen.get().get("orFilters")).isNull();
    }

    @Test
    void propagatesGroupResolverException() throws Exception {
        RuntimeException boom = new RuntimeException("group service unavailable");
        CachedGroupResolver failingGroups = mock(CachedGroupResolver.class);
        when(failingGroups.groupsFor(any(), any())).thenThrow(boom);

        OwnershipInstrumentation inst = new OwnershipInstrumentation(
                new OwnershipFilterBuilder(), new FieldArgumentMutators(), failingGroups, admins, accessResolver, true);

        DataFetcher<?> wrapped = inst.instrumentDataFetcher(
                env -> "should not be called",
                fieldFetchParams("searchAcrossEntities"),
                null);

        // Fail-closed: group-service outage propagates as an exception rather than
        // silently allowing unfiltered access.
        assertThatThrownBy(() -> wrapped.get(envFor(alice, baseInput())))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("group service unavailable");
    }

    @Test
    void injectsNoOwnersDisjunctSoUnownedAssetsAreVisible() throws Exception {
        DataFetcher<?> capturing = env ->
                new LinkedHashMap<>((Map<String, Object>) env.getArgument("input"));
        DataFetcher<?> wrapped = instrumentation.instrumentDataFetcher(capturing,
                fieldFetchParams("searchAcrossEntities"), null);

        Map<String, Object> args = (Map<String, Object>) wrapped.get(envFor(alice, baseInput()));
        List<Map<String, Object>> orFilters = (List<Map<String, Object>>) args.get("orFilters");

        // Two disjuncts: owner-match, then no-owners (EXISTS negated).
        assertThat(orFilters).hasSize(2);
        Map<String, Object> noOwners = ((List<Map<String, Object>>) orFilters.get(1).get("and")).get(0);
        assertThat(noOwners.get("field")).isEqualTo("owners");
        assertThat(noOwners.get("condition")).isEqualTo("EXISTS");
        assertThat(noOwners.get("negated")).isEqualTo(true);
    }

    @Test
    void scopesDomainSearchToAccessibleSet() throws Exception {
        AtomicReference<Map<String, Object>> seen = new AtomicReference<>();
        DataFetcher<?> capturing = env -> {
            seen.set(new LinkedHashMap<>((Map<String, Object>) env.getArgument("input")));
            return null;
        };
        DataFetcher<?> wrapped = instrumentation.instrumentDataFetcher(capturing,
                fieldFetchParams("searchAcrossEntities"), null);

        Map<String, Object> input = baseInput();
        input.put("types", List.of("DOMAIN"));
        wrapped.get(envFor(alice, input)); // alice is a non-admin; accessible domains = {CBP}

        // Domain search is restricted to the accessible domain URNs (urn IN [...]).
        List<Map<String, Object>> orFilters = (List<Map<String, Object>>) seen.get().get("orFilters");
        assertThat(orFilters).hasSize(1);
        Map<String, Object> crit = ((List<Map<String, Object>>) orFilters.get(0).get("and")).get(0);
        assertThat(crit.get("field")).isEqualTo("urn");
        assertThat((List<String>) crit.get("values")).containsExactly("urn:li:domain:CBP");
    }

    @Test
    void exemptsGlossaryAndTagQueries() throws Exception {
        AtomicReference<Map<String, Object>> seen = new AtomicReference<>();
        DataFetcher<?> capturing = env -> {
            seen.set(new LinkedHashMap<>((Map<String, Object>) env.getArgument("input")));
            return null;
        };
        DataFetcher<?> wrapped = instrumentation.instrumentDataFetcher(capturing,
                fieldFetchParams("searchAcrossEntities"), null);

        Map<String, Object> input = baseInput();
        input.put("types", List.of("GLOSSARY_TERM"));
        wrapped.get(envFor(alice, input));

        // Glossary/tags are always visible — no filter injected.
        assertThat(seen.get().get("orFilters")).isNull();
    }

    @Test
    void filtersMixedQueryContainingDataAssets() throws Exception {
        AtomicReference<Map<String, Object>> seen = new AtomicReference<>();
        DataFetcher<?> capturing = env -> {
            seen.set(new LinkedHashMap<>((Map<String, Object>) env.getArgument("input")));
            return null;
        };
        DataFetcher<?> wrapped = instrumentation.instrumentDataFetcher(capturing,
                fieldFetchParams("searchAcrossEntities"), null);

        Map<String, Object> input = baseInput();
        input.put("types", List.of("DOMAIN", "DATASET")); // contains a data asset
        wrapped.get(envFor(alice, input));

        assertThat(seen.get().get("orFilters")).isNotNull();
    }

    // ----- Helpers -----

    private InstrumentationFieldFetchParameters fieldFetchParams(String fieldName) {
        InstrumentationFieldFetchParameters params = mock(InstrumentationFieldFetchParameters.class);
        ExecutionStepInfo executionStepInfo = mock(ExecutionStepInfo.class);
        GraphQLFieldDefinition fieldDef = mock(GraphQLFieldDefinition.class);
        when(fieldDef.getName()).thenReturn(fieldName);
        when(executionStepInfo.getFieldDefinition()).thenReturn(fieldDef);
        when(params.getExecutionStepInfo()).thenReturn(executionStepInfo);
        return params;
    }

    private Map<String, Object> baseInput() {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("query", "*");
        return input;
    }

    private DataFetchingEnvironment envFor(Urn actor, Map<String, Object> input) {
        OperationContext opCtx = mock(OperationContext.class);
        QueryContext qc = mock(QueryContext.class);
        when(qc.getOperationContext()).thenReturn(opCtx);
        when(qc.getActorUrn()).thenReturn(actor.toString());

        var graphQlContext = graphql.GraphQLContext.newContext()
                .put(QueryContext.class, qc)
                .build();

        return DataFetchingEnvironmentImpl.newDataFetchingEnvironment()
                .arguments(Map.of("input", input))
                .graphQLContext(graphQlContext)
                .build();
    }

    /** Env whose authenticated actor is the given (synthetic) URN, with the supplied JWT claims. */
    private DataFetchingEnvironment envForClaims(
            String actorUrn, Map<String, Object> claims, Map<String, Object> input) {
        OperationContext opCtx = mock(OperationContext.class);
        com.datahub.authentication.Authentication auth =
                mock(com.datahub.authentication.Authentication.class);
        when(auth.getClaims()).thenReturn(claims);
        QueryContext qc = mock(QueryContext.class);
        when(qc.getOperationContext()).thenReturn(opCtx);
        when(qc.getActorUrn()).thenReturn(actorUrn);
        when(qc.getAuthentication()).thenReturn(auth);

        var graphQlContext = graphql.GraphQLContext.newContext()
                .put(QueryContext.class, qc)
                .build();

        return DataFetchingEnvironmentImpl.newDataFetchingEnvironment()
                .arguments(Map.of("input", input))
                .graphQLContext(graphQlContext)
                .build();
    }
}
