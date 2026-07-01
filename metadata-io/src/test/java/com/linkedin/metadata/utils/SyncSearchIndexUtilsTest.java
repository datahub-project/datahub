package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.UI_SOURCE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class SyncSearchIndexUtilsTest {

  @Test
  public void requiresSyncSearchIndexUpdateFalseForGraphqlWithoutUiSourceOnMetadata() {
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);

    assertFalse(
        SyncSearchIndexUtils.requiresSyncSearchIndexUpdate(
            preProcessHooks, graphqlOperationContext(), null));
  }

  @Test
  public void requiresSyncSearchIndexUpdateTrueForExplicitUiSource() {
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();

    assertTrue(
        SyncSearchIndexUtils.requiresSyncSearchIndexUpdate(
            preProcessHooks, opContext, uiSourceSystemMetadata()));
  }

  @Test
  public void requiresSyncSearchIndexUpdateFalseWhenUiDisabled() {
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(false);

    assertFalse(
        SyncSearchIndexUtils.requiresSyncSearchIndexUpdate(
            preProcessHooks, graphqlOperationContext(), uiSourceSystemMetadata()));
  }

  @Test
  public void requiresSyncSearchIndexUpdateFalseForNonGraphqlWithoutUiSource() {
    PreProcessHooks preProcessHooks = mock(PreProcessHooks.class);
    when(preProcessHooks.isUiEnabled()).thenReturn(true);
    OperationContext opContext = TestOperationContexts.Builder.builder().buildSystemContext();

    assertFalse(
        SyncSearchIndexUtils.requiresSyncSearchIndexUpdate(preProcessHooks, opContext, null));
  }

  private static SystemMetadata uiSourceSystemMetadata() {
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    return systemMetadata;
  }

  private static OperationContext graphqlOperationContext() {
    OperationContext systemContext = TestOperationContexts.Builder.builder().buildSystemContext();
    return OperationContext.asSession(
        systemContext,
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("")
            .requestAPI(RequestContext.RequestAPI.GRAPHQL)
            .requestID("addGroupMembers")
            .userAgent(""),
        mock(com.datahub.plugins.auth.authorization.Authorizer.class),
        TestOperationContexts.TEST_USER_AUTH,
        true);
  }
}
