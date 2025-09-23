package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.OwnerType;
import com.linkedin.datahub.graphql.types.corpgroup.CorpGroupType;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnerTypeBatchResolverTest {
  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private DataLoaderRegistry _mockDataLoaderRegistry;
  private Function<DataFetchingEnvironment, List<OwnerType>> _ownerTypesProvider;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _mockDataLoaderRegistry = mock(DataLoaderRegistry.class);
    _ownerTypesProvider = mock(Function.class);
    when(_dataFetchingEnvironment.getDataLoaderRegistry()).thenReturn(_mockDataLoaderRegistry);
  }

  private List<OwnerType> getRequestOwnerTypes(List<String> urnList) {
    return urnList.stream()
        .map(
            urn -> {
              if (urn.startsWith("urn:li:corpuser")) {
                CorpUser user = new CorpUser();
                user.setUrn(urn);
                return user;
              } else if (urn.startsWith("urn:li:corpGroup")) {
                CorpGroup group = new CorpGroup();
                group.setUrn(urn);
                return group;
              } else {
                throw new RuntimeException("Can't handle urn " + urn);
              }
            })
        .collect(Collectors.toList());
  }

  @Test
  public void testEmptyOwnerTypesList() throws Exception {
    when(_ownerTypesProvider.apply(any())).thenReturn(ImmutableList.of());

    OwnerTypeBatchResolver resolver =
        new OwnerTypeBatchResolver(
            ImmutableList.of(
                new CorpUserType(_entityClient, null), new CorpGroupType(_entityClient)),
            _ownerTypesProvider);

    List<OwnerType> response = resolver.get(_dataFetchingEnvironment).join();
    assertEquals(response.size(), 0);
  }

  @Test
  public void testNullOwnerTypesList() throws Exception {
    when(_ownerTypesProvider.apply(any())).thenReturn(null);

    OwnerTypeBatchResolver resolver =
        new OwnerTypeBatchResolver(
            ImmutableList.of(
                new CorpUserType(_entityClient, null), new CorpGroupType(_entityClient)),
            _ownerTypesProvider);

    List<OwnerType> response = resolver.get(_dataFetchingEnvironment).join();
    assertEquals(response.size(), 0);
  }

  @Test
  public void testMixedOwnerTypes() throws Exception {
    List<OwnerType> inputOwners =
        getRequestOwnerTypes(ImmutableList.of("urn:li:corpuser:1", "urn:li:corpGroup:1"));
    when(_ownerTypesProvider.apply(any())).thenReturn(inputOwners);

    DataLoader<Object, Object> userLoader = mock(DataLoader.class);
    DataLoader<Object, Object> groupLoader = mock(DataLoader.class);

    when(_mockDataLoaderRegistry.getDataLoader("CorpUser")).thenReturn(userLoader);
    when(_mockDataLoaderRegistry.getDataLoader("CorpGroup")).thenReturn(groupLoader);

    CorpUser mockUser = new CorpUser();
    mockUser.setUrn("urn:li:corpuser:1");

    CorpGroup mockGroup = new CorpGroup();
    mockGroup.setUrn("urn:li:corpGroup:1");

    when(userLoader.loadMany(any()))
        .thenReturn(CompletableFuture.completedFuture(ImmutableList.of(mockUser)));
    when(groupLoader.loadMany(any()))
        .thenReturn(CompletableFuture.completedFuture(ImmutableList.of(mockGroup)));

    OwnerTypeBatchResolver resolver =
        new OwnerTypeBatchResolver(
            ImmutableList.of(
                new CorpUserType(_entityClient, null), new CorpGroupType(_entityClient)),
            _ownerTypesProvider);

    List<OwnerType> response = resolver.get(_dataFetchingEnvironment).join();
    assertEquals(response.size(), 2);
    assertTrue(response.get(0) instanceof CorpUser);
    assertTrue(response.get(1) instanceof CorpGroup);
    assertEquals(((CorpUser) response.get(0)).getUrn(), "urn:li:corpuser:1");
    assertEquals(((CorpGroup) response.get(1)).getUrn(), "urn:li:corpGroup:1");
  }

  @Test
  public void testDuplicateOwnerUrns() throws Exception {
    List<OwnerType> inputOwners =
        getRequestOwnerTypes(ImmutableList.of("urn:li:corpuser:1", "urn:li:corpuser:1"));
    when(_ownerTypesProvider.apply(any())).thenReturn(inputOwners);

    DataLoader<Object, Object> userLoader = mock(DataLoader.class);
    when(_mockDataLoaderRegistry.getDataLoader("CorpUser")).thenReturn(userLoader);

    CorpUser mockUser = new CorpUser();
    mockUser.setUrn("urn:li:corpuser:1");

    when(userLoader.loadMany(any()))
        .thenReturn(CompletableFuture.completedFuture(ImmutableList.of(mockUser, mockUser)));

    OwnerTypeBatchResolver resolver =
        new OwnerTypeBatchResolver(
            ImmutableList.of(new CorpUserType(_entityClient, null)), _ownerTypesProvider);

    List<OwnerType> response = resolver.get(_dataFetchingEnvironment).join();
    assertEquals(response.size(), 2);
    assertTrue(response.get(0) instanceof CorpUser);
    assertTrue(response.get(1) instanceof CorpUser);
    assertEquals(((CorpUser) response.get(0)).getUrn(), "urn:li:corpuser:1");
    assertEquals(((CorpUser) response.get(1)).getUrn(), "urn:li:corpuser:1");
  }

  @Test
  public void testUnknownOwnerType() throws Exception {
    // Create a custom owner type that isn't registered with the resolver
    class CustomOwnerType extends CorpUser {}

    CustomOwnerType customOwner = new CustomOwnerType();
    customOwner.setUrn("urn:li:custom:1");

    when(_ownerTypesProvider.apply(any())).thenReturn(ImmutableList.of(customOwner));

    OwnerTypeBatchResolver resolver =
        new OwnerTypeBatchResolver(
            ImmutableList.of(new CorpUserType(_entityClient, null)), _ownerTypesProvider);

    List<OwnerType> response = resolver.get(_dataFetchingEnvironment).join();
    assertEquals(response.size(), 0);
  }
}
