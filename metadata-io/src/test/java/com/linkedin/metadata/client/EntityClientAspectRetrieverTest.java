package com.linkedin.metadata.client;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityClientAspectRetrieverTest {

  private static final Urn EXISTING_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.test.exists");
  private static final Urn MISSING_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.test.missing");

  private SystemEntityClient entityClient;
  private OperationContext systemOperationContext;
  private EntityClientAspectRetriever retriever;

  @BeforeMethod
  public void setup() {
    entityClient = mock(SystemEntityClient.class);
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    retriever =
        EntityClientAspectRetriever.builder()
            .entityClient(entityClient)
            .systemOperationContext(systemOperationContext)
            .build();
  }

  @Test
  public void testEntityExistsUsesFilterExistingUrns() throws RemoteInvocationException {
    when(entityClient.filterExistingUrns(systemOperationContext, Set.of(EXISTING_URN, MISSING_URN)))
        .thenReturn(Set.of(EXISTING_URN));

    Map<Urn, Boolean> result = retriever.entityExists(Set.of(EXISTING_URN, MISSING_URN));

    assertTrue(result.get(EXISTING_URN));
    assertFalse(result.get(MISSING_URN));
    verify(entityClient, times(1))
        .filterExistingUrns(eq(systemOperationContext), eq(Set.of(EXISTING_URN, MISSING_URN)));
  }

  @Test
  public void testEntityExistsEmptyInput() throws RemoteInvocationException {
    assertEquals(retriever.entityExists(Set.of()), Map.of());
    verify(entityClient, times(0)).filterExistingUrns(eq(systemOperationContext), eq(Set.of()));
  }
}
