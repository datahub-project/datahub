package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateServiceAccountDefaultViewInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.CorpUserViewsSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateServiceAccountDefaultViewResolverTest {

  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String TEST_SERVICE_ACCOUNT_URN =
      "urn:li:corpuser:service_ingestion-pipeline";
  private static final String TEST_VIEW_URN = "urn:li:dataHubView:my-view";

  private SettingsService mockSettingsService;
  private EntityClient mockEntityClient;
  private ViewService mockViewService;
  private DataFetchingEnvironment mockEnv;
  private UpdateServiceAccountDefaultViewResolver resolver;

  @BeforeMethod
  public void setup() {
    mockSettingsService = mock(SettingsService.class);
    mockEntityClient = mock(EntityClient.class);
    mockViewService = mock(ViewService.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    resolver =
        new UpdateServiceAccountDefaultViewResolver(
            mockSettingsService, mockEntityClient, mockViewService);
  }

  @Test
  public void testSetDefaultViewSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateServiceAccountDefaultViewInput input = new UpdateServiceAccountDefaultViewInput();
    input.setUrn(TEST_SERVICE_ACCOUNT_URN);
    input.setDefaultView(TEST_VIEW_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    mockServiceAccountEntity(true);
    mockViewInfo(TEST_VIEW_URN, DataHubViewType.GLOBAL);
    when(mockSettingsService.getCorpUserSettings(any(), any())).thenReturn(null);

    Boolean result = resolver.get(mockEnv).get();
    assertTrue(result);

    Urn saUrn = UrnUtils.getUrn(TEST_SERVICE_ACCOUNT_URN);
    verify(mockSettingsService)
        .updateCorpUserSettings(
            any(),
            eq(saUrn),
            argThat(
                settings ->
                    settings.hasViews()
                        && settings.getViews().hasDefaultView()
                        && settings.getViews().getDefaultView().toString().equals(TEST_VIEW_URN)));
  }

  @Test
  public void testClearDefaultViewSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateServiceAccountDefaultViewInput input = new UpdateServiceAccountDefaultViewInput();
    input.setUrn(TEST_SERVICE_ACCOUNT_URN);
    input.setDefaultView(null);
    when(mockEnv.getArgument("input")).thenReturn(input);

    mockServiceAccountEntity(true);
    CorpUserSettings existing =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setViews(new CorpUserViewsSettings().setDefaultView(UrnUtils.getUrn(TEST_VIEW_URN)));
    when(mockSettingsService.getCorpUserSettings(any(), any())).thenReturn(existing);

    Boolean result = resolver.get(mockEnv).get();
    assertTrue(result);

    verify(mockSettingsService)
        .updateCorpUserSettings(
            any(),
            eq(UrnUtils.getUrn(TEST_SERVICE_ACCOUNT_URN)),
            argThat(settings -> settings.hasViews() && !settings.getViews().hasDefaultView()));
  }

  @Test
  public void testUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateServiceAccountDefaultViewInput input = new UpdateServiceAccountDefaultViewInput();
    input.setUrn(TEST_SERVICE_ACCOUNT_URN);
    input.setDefaultView(TEST_VIEW_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    try {
      resolver.get(mockEnv).get();
      fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof AuthorizationException);
    }

    verify(mockSettingsService, never()).updateCorpUserSettings(any(), any(), any());
  }

  @Test
  public void testRejectsPersonalView() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateServiceAccountDefaultViewInput input = new UpdateServiceAccountDefaultViewInput();
    input.setUrn(TEST_SERVICE_ACCOUNT_URN);
    input.setDefaultView(TEST_VIEW_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    mockServiceAccountEntity(true);
    mockViewInfo(TEST_VIEW_URN, DataHubViewType.PERSONAL);

    try {
      resolver.get(mockEnv).get();
      fail("Expected ExecutionException for personal view");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("Only public (global) views"));
    }

    verify(mockSettingsService, never()).updateCorpUserSettings(any(), any(), any());
  }

  @Test
  public void testRejectsNonExistentView() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateServiceAccountDefaultViewInput input = new UpdateServiceAccountDefaultViewInput();
    input.setUrn(TEST_SERVICE_ACCOUNT_URN);
    input.setDefaultView("urn:li:dataHubView:does-not-exist");
    when(mockEnv.getArgument("input")).thenReturn(input);

    mockServiceAccountEntity(true);
    when(mockViewService.getViewInfo(any(), any())).thenReturn(null);

    try {
      resolver.get(mockEnv).get();
      fail("Expected ExecutionException for non-existent view");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("does not exist"));
    }

    verify(mockSettingsService, never()).updateCorpUserSettings(any(), any(), any());
  }

  @Test
  public void testTargetIsNotServiceAccount() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_ACTOR_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);

    UpdateServiceAccountDefaultViewInput input = new UpdateServiceAccountDefaultViewInput();
    input.setUrn("urn:li:corpuser:regular-user");
    input.setDefaultView(TEST_VIEW_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);

    // Return entity without SERVICE_ACCOUNT sub-type
    EntityResponse response = mock(EntityResponse.class);
    when(response.getUrn()).thenReturn(Urn.createFromString("urn:li:corpuser:regular-user"));
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    when(response.getAspects()).thenReturn(aspectMap);
    when(mockEntityClient.getV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(response);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockSettingsService, never()).updateCorpUserSettings(any(), any(), any());
  }

  private void mockServiceAccountEntity(boolean isServiceAccount) throws Exception {
    EntityResponse response = mock(EntityResponse.class);
    when(response.getUrn()).thenReturn(Urn.createFromString(TEST_SERVICE_ACCOUNT_URN));
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    if (isServiceAccount) {
      SubTypes subTypes = new SubTypes();
      subTypes.setTypeNames(new StringArray(ServiceAccountUtils.SERVICE_ACCOUNT_SUB_TYPE));
      EnvelopedAspect subTypesAspect = new EnvelopedAspect();
      subTypesAspect.setValue(new Aspect(subTypes.data()));
      aspectMap.put(Constants.SUB_TYPES_ASPECT_NAME, subTypesAspect);
    }

    when(response.getAspects()).thenReturn(aspectMap);
    when(mockEntityClient.getV2(any(), eq(Constants.CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(response);
  }

  private void mockViewInfo(String viewUrn, DataHubViewType viewType) {
    DataHubViewInfo viewInfo = mock(DataHubViewInfo.class);
    when(viewInfo.getType()).thenReturn(viewType);
    when(mockViewService.getViewInfo(any(), eq(UrnUtils.getUrn(viewUrn)))).thenReturn(viewInfo);
  }
}
