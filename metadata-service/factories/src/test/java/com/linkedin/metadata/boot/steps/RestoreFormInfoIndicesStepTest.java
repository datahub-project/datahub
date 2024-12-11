package com.linkedin.metadata.boot.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RestoreFormInfoIndicesStepTest {

  private static final String VERSION_1 = "1";
  private static final String VERSION_2 = "2";
  private static final String FORM_INFO_UPGRADE_URN =
      String.format(
          "urn:li:%s:%s", Constants.DATA_HUB_UPGRADE_ENTITY_NAME, "restore-form-info-indices");
  private final Urn formUrn = UrnUtils.getUrn("urn:li:form:test");

  @Test
  public void testExecuteFirstTime() throws Exception {
    final EntityService<?> mockService = Mockito.mock(EntityService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
    final OperationContext mockContext = mock(OperationContext.class);
    when(mockContext.getEntityRegistry()).thenReturn(mockRegistry);

    mockGetUpgradeStep(mockContext, false, VERSION_1, mockService);
    mockGetFormInfo(mockContext, formUrn, mockService);

    final AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    final RestoreFormInfoIndicesStep restoreIndicesStep =
        new RestoreFormInfoIndicesStep(mockService);
    restoreIndicesStep.execute(mockContext);

    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.FORM_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(2))
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            Mockito.eq(false));
    Mockito.verify(mockService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            any(OperationContext.class),
            Mockito.eq(formUrn),
            Mockito.eq(Constants.FORM_ENTITY_NAME),
            Mockito.eq(Constants.FORM_INFO_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            any(),
            Mockito.eq(null),
            Mockito.eq(null),
            any(),
            Mockito.eq(ChangeType.RESTATE));
  }

  @Test
  public void testExecuteWithNewVersion() throws Exception {
    final EntityService<?> mockService = Mockito.mock(EntityService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
    final OperationContext mockContext = mock(OperationContext.class);
    when(mockContext.getEntityRegistry()).thenReturn(mockRegistry);

    mockGetUpgradeStep(mockContext, true, VERSION_2, mockService);
    mockGetFormInfo(mockContext, formUrn, mockService);

    final AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    final RestoreFormInfoIndicesStep restoreIndicesStep =
        new RestoreFormInfoIndicesStep(mockService);
    restoreIndicesStep.execute(mockContext);

    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.FORM_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(2))
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            Mockito.eq(false));
    Mockito.verify(mockService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            any(OperationContext.class),
            Mockito.eq(formUrn),
            Mockito.eq(Constants.FORM_ENTITY_NAME),
            Mockito.eq(Constants.FORM_INFO_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            any(),
            Mockito.eq(null),
            Mockito.eq(null),
            any(),
            Mockito.eq(ChangeType.RESTATE));
  }

  @Test
  public void testDoesNotExecuteWithSameVersion() throws Exception {
    final EntityService<?> mockService = Mockito.mock(EntityService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);
    final OperationContext mockContext = mock(OperationContext.class);
    when(mockContext.getEntityRegistry()).thenReturn(mockRegistry);

    mockGetUpgradeStep(mockContext, true, VERSION_1, mockService);
    mockGetFormInfo(mockContext, formUrn, mockService);

    final AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    final RestoreFormInfoIndicesStep restoreIndicesStep =
        new RestoreFormInfoIndicesStep(mockService);
    restoreIndicesStep.execute(mockContext);

    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.FORM_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(AuditStamp.class),
            Mockito.eq(false));
    Mockito.verify(mockService, Mockito.times(0))
        .alwaysProduceMCLAsync(
            any(OperationContext.class),
            Mockito.eq(formUrn),
            Mockito.eq(Constants.FORM_ENTITY_NAME),
            Mockito.eq(Constants.FORM_INFO_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            any(),
            Mockito.eq(null),
            Mockito.eq(null),
            any(),
            Mockito.eq(ChangeType.RESTATE));
  }

  private void mockGetFormInfo(
      @Nonnull OperationContext mockContext,
      @Nonnull Urn formUrn,
      @Nonnull EntityService<?> mockService) {
    final List<ExtraInfo> extraInfos =
        ImmutableList.of(
            new ExtraInfo()
                .setUrn(formUrn)
                .setVersion(0L)
                .setAudit(
                    new AuditStamp()
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                        .setTime(0L)));

    when(mockService.alwaysProduceMCLAsync(
            any(OperationContext.class),
            any(Urn.class),
            Mockito.anyString(),
            Mockito.anyString(),
            any(AspectSpec.class),
            Mockito.eq(null),
            any(),
            any(),
            any(),
            any(),
            any(ChangeType.class)))
        .thenReturn(Pair.of(Mockito.mock(Future.class), false));

    when(mockService.listLatestAspects(
            any(OperationContext.class),
            Mockito.eq(Constants.FORM_ENTITY_NAME),
            Mockito.eq(Constants.FORM_INFO_ASPECT_NAME),
            Mockito.eq(0),
            Mockito.eq(1000)))
        .thenReturn(
            new ListResult<>(
                ImmutableList.of(new FormInfo()),
                new ListResultMetadata().setExtraInfos(new ExtraInfoArray(extraInfos)),
                1,
                false,
                1,
                1,
                1));
  }

  private AspectSpec mockAspectSpecs(@Nonnull EntityRegistry mockRegistry) {
    final EntitySpec entitySpec = Mockito.mock(EntitySpec.class);
    final AspectSpec aspectSpec = Mockito.mock(AspectSpec.class);
    //  Mock for formInfo
    when(mockRegistry.getEntitySpec(Constants.FORM_ENTITY_NAME)).thenReturn(entitySpec);
    when(entitySpec.getAspectSpec(Constants.FORM_INFO_ASPECT_NAME)).thenReturn(aspectSpec);

    return aspectSpec;
  }

  private void mockGetUpgradeStep(
      @Nonnull OperationContext mockContext,
      boolean shouldReturnResponse,
      @Nonnull String version,
      @Nonnull EntityService<?> mockService)
      throws Exception {

    final Urn upgradeEntityUrn = UrnUtils.getUrn(FORM_INFO_UPGRADE_URN);
    final com.linkedin.upgrade.DataHubUpgradeRequest upgradeRequest =
        new com.linkedin.upgrade.DataHubUpgradeRequest().setVersion(version);
    final Map<String, EnvelopedAspect> upgradeRequestAspects = new HashMap<>();
    upgradeRequestAspects.put(
        Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data())));
    final EntityResponse response =
        new EntityResponse().setAspects(new EnvelopedAspectMap(upgradeRequestAspects));
    when(mockService.getEntityV2(
            mockContext,
            Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
            upgradeEntityUrn,
            Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)))
        .thenReturn(shouldReturnResponse ? response : null);
  }
}
