package com.linkedin.datahub.graphql.resolvers.siblings;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SiblingGroup;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.graph.SiblingGraphService.SiblingGroupMember;
import com.linkedin.metadata.graph.SiblingGraphService.SiblingGroupMembers;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class SiblingGroupsResolverTest {

  private static final Urn ORIGINAL = dataset("orders");

  private SiblingGraphService siblingGraphService;
  private SiblingGroupsResolver resolver;
  private DataFetchingEnvironment environment;
  private QueryContext context;
  private List<SiblingGroupMember> members;

  @BeforeMethod
  public void setup() {
    siblingGraphService = mock(SiblingGraphService.class);
    resolver = new SiblingGroupsResolver(siblingGraphService);
    environment = mock(DataFetchingEnvironment.class);
    context = getMockAllowContext();
    when(environment.getContext()).thenReturn(context);
    members = new ArrayList<>();
    members.add(new SiblingGroupMember(dataset("orders_primary"), true));
    IntStream.range(0, 12)
        .forEach(i -> members.add(new SiblingGroupMember(dataset("copy_" + i), false)));
    // Every requested dataset gets the same group, so tests can request several.
    when(siblingGraphService.siblingGroupMembers(any(OperationContext.class), anyCollection()))
        .thenAnswer(
            args -> {
              Map<Urn, SiblingGroupMembers> groups = new LinkedHashMap<>();
              for (Urn urn : (Collection<Urn>) args.getArgument(1)) {
                groups.put(urn, new SiblingGroupMembers(members, false));
              }
              return groups;
            });
  }

  @Test
  public void testDefaultPageIsFiveWithTheTotal() throws Exception {
    input(Map.of("urns", List.of(ORIGINAL.toString())));

    List<SiblingGroup> groups = resolver.get(environment).get();

    assertEquals(groups.size(), 1);
    assertEquals(groups.get(0).getUrn(), ORIGINAL.toString());
    assertEquals(groups.get(0).getTotal(), 13);
    assertEquals(groups.get(0).getMembers().size(), 5);
    assertTrue(groups.get(0).getMembers().get(0).getIsPrimary());
    assertEquals(
        groups.get(0).getMembers().get(0).getEntity().getUrn(),
        dataset("orders_primary").toString());
  }

  @Test
  public void testPagingThroughTheRest() throws Exception {
    input(Map.of("urns", List.of(ORIGINAL.toString()), "start", 10, "count", 200));

    SiblingGroup group = resolver.get(environment).get().get(0);

    assertEquals(group.getTotal(), 13);
    assertEquals(
        group.getMembers().stream().map(m -> m.getEntity().getUrn()).collect(Collectors.toList()),
        List.of(
            dataset("copy_9").toString(),
            dataset("copy_10").toString(),
            dataset("copy_11").toString()));
  }

  @Test
  public void testCountIsCapped() throws Exception {
    input(Map.of("urns", List.of(ORIGINAL.toString()), "count", 5000));
    for (int i = 12; i < 400; i++) {
      members.add(new SiblingGroupMember(dataset("copy_" + i), false));
    }

    SiblingGroup group = resolver.get(environment).get().get(0);

    assertEquals(group.getMembers().size(), SiblingGroupsResolver.MAX_COUNT);
  }

  @Test
  public void testTooManyUrnsIsRejected() {
    List<String> urns =
        IntStream.range(0, context.getMaxSiblingGroupUrns() + 1)
            .mapToObj(i -> dataset("t" + i).toString())
            .collect(Collectors.toList());
    input(Map.of("urns", urns));

    assertThrows(IllegalArgumentException.class, () -> resolver.get(environment));
    verifyNoInteractions(siblingGraphService);
  }

  @Test
  public void testConfiguredUrnLimitIsApplied() throws Exception {
    when(context.getMaxSiblingGroupUrns()).thenReturn(2);
    input(Map.of("urns", List.of(dataset("a").toString(), dataset("b").toString())));
    resolver.get(environment).get();

    input(
        Map.of(
            "urns",
            List.of(dataset("a").toString(), dataset("b").toString(), dataset("c").toString())));
    assertThrows(IllegalArgumentException.class, () -> resolver.get(environment));
  }

  /** A limit of 0 turns the query off. */
  @Test
  public void testZeroLimitRejectsEveryRequest() {
    when(context.getMaxSiblingGroupUrns()).thenReturn(0);
    input(Map.of("urns", List.of(ORIGINAL.toString())));

    assertThrows(IllegalArgumentException.class, () -> resolver.get(environment));
    verifyNoInteractions(siblingGraphService);
  }

  /** Members the caller can't view are left out of both the page and the total. */
  @Test
  public void testMembersTheCallerCannotViewAreLeftOut() throws Exception {
    input(Map.of("urns", List.of(ORIGINAL.toString())));
    Urn hidden = dataset("copy_0");
    resolver = new SiblingGroupsResolver(siblingGraphService, (op, urn) -> !hidden.equals(urn));

    SiblingGroup group = resolver.get(environment).get().get(0);

    assertEquals(group.getTotal(), 12);
    assertTrue(
        group.getMembers().stream()
            .noneMatch(m -> m.getEntity().getUrn().equals(hidden.toString())));
  }

  /** A dataset the caller can't view gets an empty group and its siblings are never read. */
  @Test
  public void testDatasetTheCallerCannotViewGetsAnEmptyGroup() throws Exception {
    input(Map.of("urns", List.of(ORIGINAL.toString())));
    resolver = new SiblingGroupsResolver(siblingGraphService, (op, urn) -> !ORIGINAL.equals(urn));

    SiblingGroup group = resolver.get(environment).get().get(0);

    assertEquals(group.getTotal(), 0);
    assertTrue(group.getMembers().isEmpty());
    verifyNoInteractions(siblingGraphService);
  }

  /** Requested datasets are resolved with one call to the sibling graph. */
  @Test
  public void testOneServiceCallPerRequest() throws Exception {
    Urn other = dataset("orders_copy");
    input(Map.of("urns", List.of(ORIGINAL.toString(), other.toString())));

    List<SiblingGroup> groups = resolver.get(environment).get();

    assertEquals(groups.size(), 2);
    assertEquals(groups.get(1).getUrn(), other.toString());
    assertEquals(groups.get(1).getTotal(), 13);
    verify(siblingGraphService, times(1))
        .siblingGroupMembers(any(OperationContext.class), eq(List.of(ORIGINAL, other)));
  }

  /** Only the datasets the caller can view are walked; the others get an empty group. */
  @Test
  public void testOnlyViewableDatasetsAreWalked() throws Exception {
    Urn hidden = dataset("hidden_orders");
    input(Map.of("urns", List.of(hidden.toString(), ORIGINAL.toString())));
    resolver = new SiblingGroupsResolver(siblingGraphService, (op, urn) -> !hidden.equals(urn));

    List<SiblingGroup> groups = resolver.get(environment).get();

    assertEquals(groups.get(0).getTotal(), 0);
    assertTrue(groups.get(0).getMembers().isEmpty());
    assertEquals(groups.get(1).getTotal(), 13);
    verify(siblingGraphService, times(1))
        .siblingGroupMembers(any(OperationContext.class), eq(List.of(ORIGINAL)));
  }

  @Test
  public void testNonDatasetOrMalformedUrnIsRejected() {
    input(Map.of("urns", List.of("urn:li:chart:(looker,c1)")));
    assertThrows(IllegalArgumentException.class, () -> resolver.get(environment));
    input(Map.of("urns", List.of("not-an-urn")));
    assertThrows(IllegalArgumentException.class, () -> resolver.get(environment));
  }

  private void input(Map<String, Object> input) {
    when(environment.getArgument(eq("input"))).thenReturn(ImmutableMap.copyOf(input));
  }

  private static Urn dataset(String name) {
    return UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:glue,db." + name + ",PROD)");
  }
}
