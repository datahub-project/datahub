package com.linkedin.metadata.entity;

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
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityServiceAspectRetrieverTest {

  private static final Urn EXISTING_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.test.exists");
  private static final Urn MISSING_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.test.missing");

  private EntityService<?> entityService;
  private OperationContext systemOperationContext;
  private EntityServiceAspectRetriever retriever;

  @BeforeMethod
  public void setup() {
    entityService = mock(EntityService.class);
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    retriever =
        EntityServiceAspectRetriever.builder()
            .entityService(entityService)
            .entityRegistry(mock(EntityRegistry.class))
            .systemOperationContext(systemOperationContext)
            .build();
  }

  @Test
  public void testEntityExistsUsesBatchExists() {
    when(entityService.exists(systemOperationContext, Set.of(EXISTING_URN, MISSING_URN)))
        .thenReturn(Set.of(EXISTING_URN));

    Map<Urn, Boolean> result =
        retriever.entityExists(systemOperationContext, Set.of(EXISTING_URN, MISSING_URN));

    assertTrue(result.get(EXISTING_URN));
    assertFalse(result.get(MISSING_URN));
    verify(entityService, times(1))
        .exists(eq(systemOperationContext), eq(Set.of(EXISTING_URN, MISSING_URN)));
  }

  @Test
  public void testEntityExistsEmptyInput() {
    assertEquals(retriever.entityExists(systemOperationContext, Set.of()), Map.of());
    verify(entityService, times(0)).exists(eq(systemOperationContext), eq(Set.of()));
  }
}
