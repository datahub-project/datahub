package com.linkedin.datahub.graphql.resolvers.entity.versioning;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.UnlinkVersionInput;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UnlinkAssetVersionResolverTest {

  private static final String TEST_VERSION_SET_URN = "urn:li:versionSet:test-version-set";
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";

  @Test
  public void testGetSuccessful() throws Exception {
    EntityVersioningService mockService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);

    Mockito.when(
            mockService.unlinkVersion(
                any(),
                eq(UrnUtils.getUrn(TEST_VERSION_SET_URN)),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN))))
        .thenReturn(Collections.emptyList());

    UnlinkAssetVersionResolver resolver = new UnlinkAssetVersionResolver(mockService, mockFlags);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    UnlinkVersionInput input = new UnlinkVersionInput();
    input.setVersionSet(TEST_VERSION_SET_URN);
    input.setUnlinkedEntity(TEST_ENTITY_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertEquals(resolver.get(mockEnv).get(), null);

    Mockito.verify(mockService)
        .unlinkVersion(
            any(), eq(UrnUtils.getUrn(TEST_VERSION_SET_URN)), eq(UrnUtils.getUrn(TEST_ENTITY_URN)));
  }

  @Test
  public void testGetFeatureFlagDisabled() throws Exception {
    EntityVersioningService mockService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(false);

    UnlinkAssetVersionResolver resolver = new UnlinkAssetVersionResolver(mockService, mockFlags);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    UnlinkVersionInput input = new UnlinkVersionInput();
    input.setVersionSet(TEST_VERSION_SET_URN);
    input.setUnlinkedEntity(TEST_ENTITY_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(IllegalAccessError.class, () -> resolver.get(mockEnv));
  }

  @Test
  public void testGetInvalidVersionSetUrn() throws Exception {
    EntityVersioningService mockService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);

    UnlinkAssetVersionResolver resolver = new UnlinkAssetVersionResolver(mockService, mockFlags);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    UnlinkVersionInput input = new UnlinkVersionInput();
    input.setVersionSet("urn:li:dataset:invalid-version-set"); // Invalid URN type
    input.setUnlinkedEntity(TEST_ENTITY_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv));
  }

  @Test
  public void testGetServiceException() throws Exception {
    EntityVersioningService mockService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);

    Mockito.doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .unlinkVersion(any(), any(), any());

    UnlinkAssetVersionResolver resolver = new UnlinkAssetVersionResolver(mockService, mockFlags);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    UnlinkVersionInput input = new UnlinkVersionInput();
    input.setVersionSet(TEST_VERSION_SET_URN);
    input.setUnlinkedEntity(TEST_ENTITY_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
