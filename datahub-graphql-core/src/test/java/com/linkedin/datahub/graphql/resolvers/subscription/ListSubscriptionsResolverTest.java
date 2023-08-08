package com.linkedin.datahub.graphql.resolvers.subscription;

import com.datahub.authentication.Authentication;
import com.datahub.subscription.SubscriptionService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.ListSubscriptionsInput;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.subscription.SubscriptionTestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ListSubscriptionsResolverTest {
  private static final DataHubSubscription MAPPED_SUBSCRIPTION_1 = getMappedSubscription1();
  private static final DataHubSubscription MAPPED_SUBSCRIPTION_2 = getMappedSubscription2();
  private ListSubscriptionsResolver _resolver;
  private SubscriptionService _subscriptionService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _subscriptionService = mock(SubscriptionService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    final ListSubscriptionsInput input = new ListSubscriptionsInput();
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    _resolver = new ListSubscriptionsResolver(_subscriptionService);
  }

  @Test
  public void testListSubscriptionsExceptionThrown() {
    when(_subscriptionService.listSubscriptions(any(SearchResult.class), eq(_authentication))).thenThrow(
        new RuntimeException("Failed to list subscriptions"));

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testListSubscriptions() throws Exception {
    SearchResult searchResult = new  SearchResult()
        .setNumEntities(2)
        .setEntities(
            new SearchEntityArray(
                ImmutableSet.of(new SearchEntity().setEntity(SUBSCRIPTION_URN_1), new SearchEntity().setEntity(SUBSCRIPTION_URN_2))
            )
        );
    when(_subscriptionService.getSubscriptionsSearchResult(eq(USER_URN), anyInt(), anyInt(), eq(_authentication))).thenReturn(searchResult);
    when(_subscriptionService.listSubscriptions(any(SearchResult.class), eq(_authentication))).thenReturn(
        ImmutableMap.of(SUBSCRIPTION_URN_1, SUBSCRIPTION_INFO_1, SUBSCRIPTION_URN_2, SUBSCRIPTION_INFO_2));

    final List<DataHubSubscription> subscriptions =
        _resolver.get(_dataFetchingEnvironment).join().getSubscriptions().stream().sorted(
            Comparator.comparing(DataHubSubscription::getSubscriptionUrn)).collect(Collectors.toList());
    final DataHubSubscriptionMatcher matcher1 = new DataHubSubscriptionMatcher(MAPPED_SUBSCRIPTION_1);
    assertTrue(matcher1.matches(subscriptions.get(0)));
    final DataHubSubscriptionMatcher matcher2 = new DataHubSubscriptionMatcher(MAPPED_SUBSCRIPTION_2);
    assertTrue(matcher2.matches(subscriptions.get(1)));
  }
}
