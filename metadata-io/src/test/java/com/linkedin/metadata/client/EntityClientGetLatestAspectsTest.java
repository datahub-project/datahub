package com.linkedin.metadata.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DatasetKey;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that {@link EntityClient#getLatestAspects} routes through the 4-arg {@code batchGetV2}
 * (cacheable) rather than the 5-arg overload, and that {@code alwaysIncludeKeyAspect=true}
 * synthesizes the key aspect client-side without polluting the cache key.
 */
public class EntityClientGetLatestAspectsTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)");

  private EntityClient entityClient;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    entityClient =
        mock(EntityClient.class, withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testGetLatestAspectsRoutesThroughCachedOverload() throws Exception {
    when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(Map.of());

    entityClient.getLatestAspects(opContext, Set.of(DATASET_URN), Set.of("ownership"), false);

    // Must call the 4-arg (cached) overload with unmodified aspectNames
    verify(entityClient)
        .batchGetV2(eq(opContext), eq("dataset"), eq(Set.of(DATASET_URN)), eq(Set.of("ownership")));
  }

  @Test
  public void testGetLatestAspectsWithKeyAspectSynthesizesWithoutCachePollution() throws Exception {
    when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(Map.of());

    Map<Urn, Map<String, Aspect>> result =
        entityClient.getLatestAspects(opContext, Set.of(DATASET_URN), Set.of("ownership"), true);

    // batchGetV2 called with original aspectNames — key aspect NOT added to the cache key
    verify(entityClient)
        .batchGetV2(eq(opContext), eq("dataset"), eq(Set.of(DATASET_URN)), eq(Set.of("ownership")));

    // Key aspect synthesized in the result
    Map<String, Aspect> aspects = result.get(DATASET_URN);
    assertNotNull(aspects);
    Aspect keyAspect = aspects.get("datasetKey");
    assertNotNull(keyAspect, "Key aspect should be synthesized when alwaysIncludeKeyAspect=true");
    assertEquals(aspects.size(), 1, "Only the synthesized key aspect should be present");

    DatasetUrn datasetUrn = DatasetUrn.createFromUrn(DATASET_URN);
    DatasetKey datasetKey = new DatasetKey(keyAspect.data());
    assertEquals(datasetKey.getPlatform(), datasetUrn.getPlatformEntity());
    assertEquals(datasetKey.getName(), datasetUrn.getDatasetNameEntity());
    assertEquals(datasetKey.getOrigin(), datasetUrn.getOriginEntity());
  }

  @Test
  public void testGetLatestSystemAspectRoutesThroughCachedOverload() throws Exception {
    when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(Map.of());

    entityClient.getLatestSystemAspect(
        opContext, Set.of(DATASET_URN), Set.of("schemaMetadata"), false);

    verify(entityClient)
        .batchGetV2(
            eq(opContext), eq("dataset"), eq(Set.of(DATASET_URN)), eq(Set.of("schemaMetadata")));
  }

  @Test
  public void testGetLatestSystemAspectWithKeyAspectCallsUncachedOverload() throws Exception {
    when(entityClient.batchGetV2(any(), any(), any(), any(), any())).thenReturn(Map.of());

    entityClient.getLatestSystemAspect(
        opContext, Set.of(DATASET_URN), Set.of("schemaMetadata"), true);

    // Must call the 5-arg (uncached) overload since SystemAspect needs DB metadata
    verify(entityClient)
        .batchGetV2(
            eq(opContext),
            eq("dataset"),
            eq(Set.of(DATASET_URN)),
            eq(Set.of("schemaMetadata")),
            eq(true));
  }
}
