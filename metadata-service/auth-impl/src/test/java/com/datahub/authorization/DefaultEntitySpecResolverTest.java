package com.datahub.authorization;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.group.GroupService;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.stream.Stream;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DefaultEntitySpecResolverTest {

  private static final OperationContext BASE =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @DataProvider
  public Object[][] missingResources() {
    return Stream.concat(
            BASE.getEntityRegistry().getEntitySpecs().keySet().stream()
                .map(type -> new Object[] {type, "urn:li:" + type + ":missing"}),
            Stream.of(new Object[] {"dataset", ""}, new Object[] {"dataset", "not-a-urn"}))
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "missingResources")
  public void testResolveMissingMatchesProvidersWithNothingStored(String type, String urn) {
    final OperationContext nothingStored =
        BASE.toBuilder()
            .retrieverContext(
                RetrieverContext.builder()
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
                    .aspectRetriever(mock(AspectRetriever.class))
                    .build())
            .build(BASE.getSessionAuthentication(), false);
    final DefaultEntitySpecResolver resolver =
        new DefaultEntitySpecResolver(
            nothingStored, mock(SystemEntityClient.class), mock(GroupService.class));
    final EntitySpec spec = new EntitySpec(type, urn);

    final ResolvedEntitySpec resolved = resolver.resolve(spec);
    final ResolvedEntitySpec missing = DefaultEntitySpecResolver.resolveMissing(spec);

    for (EntityFieldType fieldType : EntityFieldType.values()) {
      assertEquals(
          missing.getFieldValues(fieldType), resolved.getFieldValues(fieldType), fieldType.name());
    }
    assertEquals(missing.getStructuredPropertyValues(), resolved.getStructuredPropertyValues());
  }
}
