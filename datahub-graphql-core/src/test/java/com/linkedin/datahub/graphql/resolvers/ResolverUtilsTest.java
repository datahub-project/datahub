package com.linkedin.datahub.graphql.resolvers;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.search.utils.QueryUtils.buildFilterWithUrns;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ResolverUtilsTest {

  @Test
  public void testCriterionFromFilter() throws Exception {
    final DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    final QueryContext mockAllowContext = TestUtils.getMockAllowContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockAllowContext);

    // this is the expected path
    Criterion valuesCriterion =
        criterionFromFilter(
            new FacetFilterInput(
                "tags",
                null,
                ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def"),
                false,
                FilterOperator.EQUAL));
    assertEquals(
        valuesCriterion,
        buildCriterion(
            "tags", Condition.EQUAL, ImmutableList.of("urn:li:tag:abc", "urn:li:tag:def")));

    // this is the legacy pathway
    Criterion valueCriterion =
        criterionFromFilter(
            new FacetFilterInput("tags", "urn:li:tag:abc", null, true, FilterOperator.EQUAL));
    assertEquals(valueCriterion, buildCriterion("tags", Condition.EQUAL, true, "urn:li:tag:abc"));

    // check that both being null doesn't cause a NPE. this should never happen except via API
    // interaction
    Criterion doubleNullCriterion =
        criterionFromFilter(new FacetFilterInput("tags", null, null, true, FilterOperator.EQUAL));
    assertEquals(
        doubleNullCriterion, buildCriterion("tags", Condition.EQUAL, true, ImmutableList.of()));
  }

  @Test
  public void testBuildFilterWithUrns() throws Exception {
    Urn urn1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz1)");
    Urn urn2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz2)");
    Set<Urn> urns = new HashSet<>();
    urns.add(urn1);
    urns.add(urn2);

    Criterion ownersCriterion = buildCriterion("owners", Condition.EQUAL, "urn:li:corpuser:chris");

    CriterionArray andCriterionArray = new CriterionArray(ImmutableList.of(ownersCriterion));
    final Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(
            ImmutableList.of(new ConjunctiveCriterion().setAnd(andCriterionArray))));

    DataHubAppConfiguration appConfig = new DataHubAppConfiguration();
    appConfig.setMetadataChangeProposal(new MetadataChangeProposalConfig());
    appConfig
        .getMetadataChangeProposal()
        .setSideEffects(new MetadataChangeProposalConfig.SideEffectsConfig());
    appConfig
        .getMetadataChangeProposal()
        .getSideEffects()
        .setSchemaField(new MetadataChangeProposalConfig.SideEffectConfig());
    appConfig.getMetadataChangeProposal().getSideEffects().getSchemaField().setEnabled(true);

    Filter finalFilter = buildFilterWithUrns(appConfig, urns, filter);

    Criterion urnsCriterion =
        buildCriterion(
            "urn",
            Condition.EQUAL,
            urns.stream().map(Object::toString).collect(Collectors.toList()));

    for (ConjunctiveCriterion conjunctiveCriterion : finalFilter.getOr()) {
      assertEquals(conjunctiveCriterion.getAnd().contains(ownersCriterion), true);
      assertEquals(conjunctiveCriterion.getAnd().contains(urnsCriterion), true);
    }
  }
}
