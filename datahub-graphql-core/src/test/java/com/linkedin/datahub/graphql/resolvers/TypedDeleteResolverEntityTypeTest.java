package com.linkedin.datahub.graphql.resolvers;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.group.GroupService;
import com.datahub.authentication.post.PostService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.resolvers.auth.DeleteServiceAccountResolver;
import com.linkedin.datahub.graphql.resolvers.businessattribute.DeleteBusinessAttributeResolver;
import com.linkedin.datahub.graphql.resolvers.dataproduct.DeleteDataProductResolver;
import com.linkedin.datahub.graphql.resolvers.domain.DeleteDomainResolver;
import com.linkedin.datahub.graphql.resolvers.form.DeleteFormResolver;
import com.linkedin.datahub.graphql.resolvers.group.RemoveGroupResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.secret.DeleteSecretResolver;
import com.linkedin.datahub.graphql.resolvers.ingest.source.DeleteIngestionSourceResolver;
import com.linkedin.datahub.graphql.resolvers.ownership.DeleteOwnershipTypeResolver;
import com.linkedin.datahub.graphql.resolvers.policy.DeletePolicyResolver;
import com.linkedin.datahub.graphql.resolvers.post.DeletePostResolver;
import com.linkedin.datahub.graphql.resolvers.structuredproperties.DeleteStructuredPropertyResolver;
import com.linkedin.datahub.graphql.resolvers.tag.DeleteTagResolver;
import com.linkedin.datahub.graphql.resolvers.test.DeleteTestResolver;
import com.linkedin.datahub.graphql.resolvers.user.RemoveUserResolver;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.service.OwnershipTypeService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Typed delete resolvers authorize a type-specific privilege, and the entity client does not
 * authorize deletes. A caller holding every privilege must still be unable to delete an entity of
 * another type through them.
 */
public class TypedDeleteResolverEntityTypeTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");

  private record Case(
      String name,
      DataFetcher<? extends CompletableFuture<?>> resolver,
      Class<? extends Throwable> expected,
      Runnable verifyNotDeleted) {
    @Override
    public String toString() {
      return name;
    }
  }

  @DataProvider(name = "resolvers")
  public Object[][] resolvers() throws Exception {
    return new Object[][] {
      {entityClientCase("deleteTag", DeleteTagResolver::new)},
      {entityClientCase("deleteDomain", DeleteDomainResolver::new)},
      {entityClientCase("deleteStructuredProperty", DeleteStructuredPropertyResolver::new)},
      {entityClientCase("deleteForm", DeleteFormResolver::new)},
      {entityClientCase("deleteBusinessAttribute", DeleteBusinessAttributeResolver::new)},
      {entityClientCase("deleteTest", DeleteTestResolver::new)},
      {entityClientCase("deleteSecret", DeleteSecretResolver::new)},
      {entityClientCase("deleteIngestionSource", DeleteIngestionSourceResolver::new)},
      {entityClientCase("deletePolicy", DeletePolicyResolver::new)},
      {entityClientCase("removeUser", RemoveUserResolver::new)},
      {
        entityClientCase(
            "removeGroup",
            client -> new RemoveGroupResolver(client, Mockito.mock(GroupService.class)))
      },
      {deleteServiceAccountCase()},
      {deleteOwnershipTypeCase()},
      {deletePostCase()},
      {deleteDataProductCase()},
    };
  }

  @Test(dataProvider = "resolvers")
  public void testRejectsUrnOfAnotherEntityType(Case testCase) {
    QueryContext context = getMockAllowContext();
    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getContext()).thenReturn(context);
    Mockito.when(env.getArgument("urn")).thenReturn(DATASET_URN.toString());
    Mockito.when(env.getArgument("input")).thenReturn(Map.of("urn", DATASET_URN.toString()));

    Throwable thrown = expectThrows(Throwable.class, () -> testCase.resolver().get(env).join());

    assertTrue(
        hasCause(thrown, testCase.expected()),
        testCase.name() + " should fail with " + testCase.expected().getSimpleName());
    testCase.verifyNotDeleted().run();
  }

  private interface EntityClientResolverFactory {
    DataFetcher<? extends CompletableFuture<?>> create(EntityClient client);
  }

  private static Case entityClientCase(String name, EntityClientResolverFactory factory)
      throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(client.exists(any(), any())).thenReturn(true);
    return new Case(
        name,
        factory.create(client),
        AuthorizationException.class,
        () -> verifyEntityClientNotDeleted(client));
  }

  private static Case deleteServiceAccountCase() {
    EntityClient client = Mockito.mock(EntityClient.class);
    return new Case(
        "deleteServiceAccount",
        new DeleteServiceAccountResolver(client),
        IllegalArgumentException.class,
        () -> verifyEntityClientNotDeleted(client));
  }

  private static Case deleteOwnershipTypeCase() {
    OwnershipTypeService service = Mockito.mock(OwnershipTypeService.class);
    return new Case(
        "deleteOwnershipType",
        new DeleteOwnershipTypeResolver(service),
        AuthorizationException.class,
        () ->
            Mockito.verify(service, Mockito.never())
                .deleteOwnershipType(any(), any(), anyBoolean()));
  }

  private static Case deletePostCase() {
    PostService service = Mockito.mock(PostService.class);
    return new Case(
        "deletePost",
        new DeletePostResolver(service),
        AuthorizationException.class,
        () -> {
          try {
            Mockito.verify(service, Mockito.never()).deletePost(any(), any());
          } catch (Exception e) {
            throw new AssertionError(e);
          }
        });
  }

  private static Case deleteDataProductCase() {
    DataProductService service = Mockito.mock(DataProductService.class);
    Mockito.when(service.verifyEntityExists(any(), any())).thenReturn(true);
    return new Case(
        "deleteDataProduct",
        new DeleteDataProductResolver(service),
        AuthorizationException.class,
        () -> Mockito.verify(service, Mockito.never()).deleteDataProduct(any(), any()));
  }

  private static void verifyEntityClientNotDeleted(EntityClient client) {
    try {
      Mockito.verify(client, Mockito.never()).deleteEntity(any(), any());
      Mockito.verify(client, Mockito.never()).deleteReferencesThenEntity(any(), any());
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static boolean hasCause(Throwable thrown, Class<? extends Throwable> expected) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (expected.isInstance(t)) {
        return true;
      }
    }
    return false;
  }
}
