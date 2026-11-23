package com.linkedin.metadata.structuredproperties.validators;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.structuredproperties.validation.HidePropertyValidator;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.StructuredPropertySettings;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HidePropertyValidatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  private static final Urn TEST_PROPERTY_URN =
      UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.privacy.retentionTime");

  private static final HidePropertyValidator VALIDATOR =
      new HidePropertyValidator()
          .setConfig(
              AspectPluginConfig.builder()
                  .className(HidePropertyValidator.class.getName())
                  .enabled(true)
                  .supportedOperations(
                      List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH"))
                  .supportedEntityAspectNames(
                      List.of(
                          AspectPluginConfig.EntityAspectName.builder()
                              .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                              .aspectName(STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME)
                              .build()))
                  .build());

  @Test
  public void testValidUpsert() {

    StructuredPropertySettings propertySettings =
        new StructuredPropertySettings()
            .setIsHidden(false)
            .setShowAsAssetBadge(true)
            .setShowInAssetSummary(true)
            .setShowInSearchFilters(true);

    boolean isValid =
        HidePropertyValidator.validateSettingsUpserts(
                TestMCP.ofOneUpsertItem(TEST_PROPERTY_URN, propertySettings, TEST_REGISTRY))
            .findAny()
            .isEmpty();
    Assert.assertTrue(isValid);
  }

  @Test
  public void testInvalidUpsert() {

    StructuredPropertySettings propertySettings =
        new StructuredPropertySettings()
            .setIsHidden(true)
            .setShowAsAssetBadge(true)
            .setShowInAssetSummary(true)
            .setShowInSearchFilters(true);

    boolean isValid =
        HidePropertyValidator.validateSettingsUpserts(
                TestMCP.ofOneUpsertItem(TEST_PROPERTY_URN, propertySettings, TEST_REGISTRY))
            .findAny()
            .isEmpty();
    Assert.assertFalse(isValid);
  }

  /**
   * A PATCH has no merged aspect at the request stage: a write-path patch item has no aspect at
   * all, and a raw proposal would read the patch body as the aspect. Neither is checked there.
   */
  @Test
  public void testPatchNotCheckedAtRequestStage() {
    for (boolean alternateValidation : new boolean[] {true, false}) {
      Assert.assertEquals(
          VALIDATOR
              .validateProposed(
                  OperationFingerprint.EMPTY,
                  Set.of(hideAndShowInFiltersPatch(alternateValidation)),
                  mock(RetrieverContext.class),
                  null)
              .count(),
          0);
    }
  }

  @Test
  public void testAppliedPatchMakingSettingsInvalidRejected() {
    Assert.assertEquals(
        preCommitCount(
            appliedPatch(
                new StructuredPropertySettings().setShowInSearchFilters(true),
                new StructuredPropertySettings().setShowInSearchFilters(true).setIsHidden(true))),
        1);
  }

  /** Settings that were already invalid must not block an unrelated patch. */
  @Test
  public void testAppliedPatchKeepingInvalidSettingsAllowed() throws CloneNotSupportedException {
    StructuredPropertySettings invalid =
        new StructuredPropertySettings().setShowInSearchFilters(true).setIsHidden(true);
    Assert.assertEquals(
        preCommitCount(appliedPatch(invalid, invalid.copy().setShowInAssetSummary(true))), 0);
  }

  @Test
  public void testPreCommitIgnoresNonPatchWrites() {
    TestMCP item =
        appliedPatch(
            new StructuredPropertySettings(),
            new StructuredPropertySettings().setShowInSearchFilters(true).setIsHidden(true));
    Assert.assertEquals(preCommitCount(item.toBuilder().metadataChangeProposal(null).build()), 0);
  }

  private static long preCommitCount(TestMCP item) {
    return VALIDATOR
        .validatePreCommit(
            OperationFingerprint.EMPTY, List.<ChangeMCP>of(item), mock(RetrieverContext.class))
        .count();
  }

  static TestMCP appliedPatch(
      StructuredPropertySettings previous, StructuredPropertySettings patched) {
    SystemAspect previousSystemAspect = mock(SystemAspect.class);
    when(previousSystemAspect.getRecordTemplate()).thenReturn(previous);
    return TestMCP.builder()
        .changeType(ChangeType.UPSERT)
        .urn(TEST_PROPERTY_URN)
        .entitySpec(TEST_REGISTRY.getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME))
        .aspectSpec(
            TEST_REGISTRY
                .getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME)
                .getAspectSpec(STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME))
        .recordTemplate(patched)
        .previousSystemAspect(previousSystemAspect)
        .metadataChangeProposal(new MetadataChangeProposal().setChangeType(ChangeType.PATCH))
        .build();
  }

  /** A PATCH setting isHidden and showInSearchFilters, as the write path or a raw proposal. */
  static BatchItem hideAndShowInFiltersPatch(boolean alternateValidation) {
    GenericJsonPatch.PatchOp hide = new GenericJsonPatch.PatchOp();
    hide.setOp("add");
    hide.setPath("/isHidden");
    hide.setValue(true);
    GenericJsonPatch.PatchOp showInFilters = new GenericJsonPatch.PatchOp();
    showInFilters.setOp("add");
    showInFilters.setPath("/showInSearchFilters");
    showInFilters.setValue(true);
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(TEST_PROPERTY_URN)
            .setEntityType(STRUCTURED_PROPERTY_ENTITY_NAME)
            .setAspectName(STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME)
            .setChangeType(ChangeType.PATCH)
            .setAspect(
                GenericRecordUtils.serializePatch(
                    GenericJsonPatch.builder().patch(List.of(hide, showInFilters)).build(),
                    new ObjectMapper()));
    return alternateValidation
        ? ProposedItem.builder()
            .build(mcp, AuditStampUtils.createDefaultAuditStamp(), TEST_REGISTRY)
        : PatchItemImpl.builder()
            .build(mcp, AuditStampUtils.createDefaultAuditStamp(), TEST_REGISTRY);
  }
}
