package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalViewsSettings;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test the behavior of IngestDefaultGlobalSettingsStep.
 *
 * <p>We expect it to ingest a JSON file, throwing if the JSON file is malformed or does not match
 * the PDL model for GlobalSettings.pdl.
 */
public class IngestDefaultGlobalSettingsStepTest {

  @Test
  public void testExecuteValidSettingsNoExistingSettings() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    configureEntityServiceMock(entityService, null);

    final IngestDefaultGlobalSettingsStep step =
        new IngestDefaultGlobalSettingsStep(
            entityService, "./boot/test_global_settings_valid.json");

    step.execute();

    GlobalSettingsInfo expectedResult = new GlobalSettingsInfo();
    expectedResult.setViews(
        new GlobalViewsSettings().setDefaultView(UrnUtils.getUrn("urn:li:dataHubView:test")));

    Mockito.verify(entityService, times(1))
        .ingestProposal(
            Mockito.eq(buildUpdateSettingsProposal(expectedResult)),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
  }

  @Test
  public void testExecuteValidSettingsExistingSettings() throws Exception {

    // Verify that the user provided settings overrides are NOT overwritten.
    final EntityService entityService = mock(EntityService.class);
    final GlobalSettingsInfo existingSettings =
        new GlobalSettingsInfo()
            .setViews(
                new GlobalViewsSettings()
                    .setDefaultView(UrnUtils.getUrn("urn:li:dataHubView:custom")));
    configureEntityServiceMock(entityService, existingSettings);

    final IngestDefaultGlobalSettingsStep step =
        new IngestDefaultGlobalSettingsStep(
            entityService, "./boot/test_global_settings_valid.json");

    step.execute();

    // Verify that the merge preserves the user settings.
    GlobalSettingsInfo expectedResult = new GlobalSettingsInfo();
    expectedResult.setViews(
        new GlobalViewsSettings().setDefaultView(UrnUtils.getUrn("urn:li:dataHubView:custom")));

    Mockito.verify(entityService, times(1))
        .ingestProposal(
            Mockito.eq(buildUpdateSettingsProposal(expectedResult)),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
  }

  @Test
  public void testExecuteInvalidJsonSettings() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    configureEntityServiceMock(entityService, null);

    final IngestDefaultGlobalSettingsStep step =
        new IngestDefaultGlobalSettingsStep(
            entityService, "./boot/test_global_settings_invalid_json.json");

    Assert.assertThrows(RuntimeException.class, step::execute);

    // Verify no interactions
    verifyNoInteractions(entityService);
  }

  @Test
  public void testExecuteInvalidModelSettings() throws Exception {
    final EntityService entityService = mock(EntityService.class);
    configureEntityServiceMock(entityService, null);

    final IngestDefaultGlobalSettingsStep step =
        new IngestDefaultGlobalSettingsStep(
            entityService, "./boot/test_global_settings_invalid_model.json");

    Assert.assertThrows(RuntimeException.class, step::execute);

    // Verify no interactions
    verifyNoInteractions(entityService);
  }

  private static void configureEntityServiceMock(
      final EntityService mockService, final GlobalSettingsInfo settingsInfo) {
    Mockito.when(
            mockService.getAspect(
                Mockito.eq(GLOBAL_SETTINGS_URN),
                Mockito.eq(GLOBAL_SETTINGS_INFO_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(settingsInfo);
  }

  private static MetadataChangeProposal buildUpdateSettingsProposal(
      final GlobalSettingsInfo settings) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(GLOBAL_SETTINGS_URN);
    mcp.setEntityType(GLOBAL_SETTINGS_ENTITY_NAME);
    mcp.setAspectName(GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(settings));
    return mcp;
  }
}
