package com.linkedin.datahub.graphql.resolvers.entity.versioning;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.LinkVersionInput;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.entity.versioning.VersionPropertiesInput;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class LinkAssetVersionResolverTest {

  private static final String TEST_VERSION_SET_URN = "urn:li:versionSet:test-version-set";
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";

  @Test
  public void testGetSuccessful() throws Exception {
    EntityVersioningService mockService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);

    IngestResult mockResult =
        IngestResult.builder().urn(Urn.createFromString(TEST_ENTITY_URN)).build();

    Mockito.when(
            mockService.linkLatestVersion(
                any(),
                eq(UrnUtils.getUrn(TEST_VERSION_SET_URN)),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                any(VersionPropertiesInput.class)))
        .thenReturn(ImmutableList.of(mockResult));

    LinkAssetVersionResolver resolver = new LinkAssetVersionResolver(mockService, mockFlags);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    LinkVersionInput input = new LinkVersionInput();
    input.setVersionSet(TEST_VERSION_SET_URN);
    input.setLinkedEntity(TEST_ENTITY_URN);
    input.setComment("Test comment");
    input.setVersion("v1");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertEquals(resolver.get(mockEnv).get().getUrn(), TEST_VERSION_SET_URN);
  }

  @Test
  public void testGetFeatureFlagDisabled() throws Exception {
    EntityVersioningService mockService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(false);

    LinkAssetVersionResolver resolver = new LinkAssetVersionResolver(mockService, mockFlags);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    LinkVersionInput input = new LinkVersionInput();
    input.setVersionSet(TEST_VERSION_SET_URN);
    input.setLinkedEntity(TEST_ENTITY_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(IllegalAccessError.class, () -> resolver.get(mockEnv));
  }

  @Test
  public void testGetInvalidVersionSetUrn() throws Exception {
    EntityVersioningService mockService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);

    LinkAssetVersionResolver resolver = new LinkAssetVersionResolver(mockService, mockFlags);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    LinkVersionInput input = new LinkVersionInput();
    input.setVersionSet("urn:li:dataset:invalid-version-set"); // Invalid URN type
    input.setLinkedEntity(TEST_ENTITY_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv));
  }
}
