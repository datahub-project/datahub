package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.VersionTag;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class VersionPropertiesAuthorizationValidatorTest {
  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  // Entity the caller may edit.
  private static final Urn ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,entity,PROD)");
  private static final Urn SECOND_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,second-entity,PROD)");
  // Version set and its current latest version, neither editable by the caller.
  private static final Urn OTHER_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(other-set,dataset)");
  private static final Urn OTHER_LATEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,other-latest,PROD)");
  // Version set whose latest version is ENTITY_URN.
  private static final Urn OWN_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(own-set,dataset)");
  private static final Urn NEW_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(new-set,dataset)");

  private VersionPropertiesAuthorizationValidator validator;
  private AuthorizationSession session;
  private MockedStatic<AuthUtil> authUtil;
  private RetrieverContext retrieverContext;
  private MockAspectRetriever aspectRetriever;

  @BeforeMethod
  public void setup() {
    authUtil = Mockito.mockStatic(AuthUtil.class);
    session = mock(AuthorizationSession.class);
    validator =
        new VersionPropertiesAuthorizationValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(VersionPropertiesAuthorizationValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT", "PATCH"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("*")
                                .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
                                .build()))
                    .build());

    aspectRetriever =
        new MockAspectRetriever(
            Map.<Urn, List<RecordTemplate>>of(
                OTHER_VERSION_SET_URN,
                List.of(setProperties(OTHER_LATEST_URN)),
                OTHER_LATEST_URN,
                List.of(versionProperties(OTHER_VERSION_SET_URN, "v1", "bbb")),
                OWN_VERSION_SET_URN,
                List.of(setProperties(ENTITY_URN)),
                ENTITY_URN,
                List.of(versionProperties(OWN_VERSION_SET_URN, "v1", "aaa"))));
    aspectRetriever.setEntityRegistry(TEST_REGISTRY);
    aspectRetriever = Mockito.spy(aspectRetriever);
    retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .searchRetriever(mock(SearchRetriever.class))
            .graphRetriever(mock(GraphRetriever.class))
            .build();
  }

  @AfterMethod
  public void tearDown() {
    authUtil.close();
  }

  @Test
  public void testNewLatestRequiresEditOnVersionSet() {
    allowEdit(ENTITY_URN);
    Assert.assertFalse(
        validate(upsert(OTHER_VERSION_SET_URN, "v2", "ccc")).isEmpty(),
        "Linking into a version set requires edit on the version set");
  }

  @Test
  public void testNewLatestRequiresEditOnDemotedPreviousLatest() {
    allowEdit(ENTITY_URN, OTHER_VERSION_SET_URN);
    Assert.assertFalse(
        validate(upsert(OTHER_VERSION_SET_URN, "v2", "ccc")).isEmpty(),
        "Becoming the latest version rewrites the previous latest, which requires edit on it");
  }

  @Test
  public void testNewLatestAllowedWithEditOnVersionSetAndPreviousLatest() {
    allowEdit(ENTITY_URN, OTHER_VERSION_SET_URN, OTHER_LATEST_URN);
    Assert.assertTrue(validate(upsert(OTHER_VERSION_SET_URN, "v2", "ccc")).isEmpty());
  }

  @Test
  public void testOlderVersionDoesNotRequireEditOnLatest() {
    allowEdit(ENTITY_URN, OTHER_VERSION_SET_URN);
    Assert.assertTrue(validate(upsert(OTHER_VERSION_SET_URN, "v0", "aaa")).isEmpty());
  }

  @Test
  public void testCreatingVersionSetRequiresEditOnIt() {
    allowEdit(ENTITY_URN);
    Assert.assertFalse(validate(upsert(NEW_VERSION_SET_URN, "v1", "aaa")).isEmpty());
  }

  @Test
  public void testUnchangedReingestOfLatestIsAllowed() {
    allowEdit(ENTITY_URN);
    Assert.assertTrue(validate(upsert(OWN_VERSION_SET_URN, "v1", "aaa")).isEmpty());
  }

  // Under alternate MCP validation a PATCH arrives as a ProposedItem; otherwise (the default) as a
  // PatchItemImpl. Both must be resolved before authorizing.
  @DataProvider(name = "patchItemBuilders")
  public Object[][] patchItemBuilders() {
    return new Object[][] {{true}, {false}};
  }

  @Test(dataProvider = "patchItemBuilders")
  public void testPatchMovingToAnotherVersionSetIsChecked(boolean alternateValidation) {
    allowEdit(ENTITY_URN);
    BatchItem patch =
        patch(
            ENTITY_URN,
            alternateValidation,
            false,
            patchOp("/versionSet", OTHER_VERSION_SET_URN.toString()),
            patchOp("/sortId", "ccc"));
    Assert.assertFalse(validate(patch).isEmpty());
  }

  @Test(dataProvider = "patchItemBuilders")
  public void testPatchCreatingVersionPropertiesIsChecked(boolean alternateValidation) {
    // SECOND_ENTITY_URN has no stored versionProperties. A template patch would be rejected, but a
    // client can set forceGenericPatch, which builds the aspect from scratch.
    allowEdit(SECOND_ENTITY_URN);
    BatchItem patch =
        patch(
            SECOND_ENTITY_URN,
            alternateValidation,
            true,
            patchOp("/versionSet", OTHER_VERSION_SET_URN.toString()),
            patchOp("/versioningScheme", VersioningScheme.LEXICOGRAPHIC_STRING.name()),
            patchOp("/version", Map.of("versionTag", "v2")),
            patchOp("/sortId", "ccc"));
    Assert.assertFalse(validate(patch).isEmpty());
  }

  // The write path applies a PATCH on top of an earlier item for the same aspect in the batch, so
  // the validator must too: a low-sortId UPSERT followed by a sortId PATCH still demotes the
  // latest.
  @Test(dataProvider = "patchItemBuilders")
  public void testSameBatchPatchOnNewEntityIsCheckedAgainstEarlierUpsert(
      boolean alternateValidation) {
    allowEdit(SECOND_ENTITY_URN, OTHER_VERSION_SET_URN);
    List<BatchItem> batch =
        List.of(
            upsertFor(SECOND_ENTITY_URN, OTHER_VERSION_SET_URN, "v0", "aaa"),
            patch(SECOND_ENTITY_URN, alternateValidation, false, patchOp("/sortId", "zzz")));
    Assert.assertFalse(validate(batch).isEmpty());
  }

  @Test(dataProvider = "patchItemBuilders")
  public void testSameBatchPatchOnExistingEntityIsCheckedAgainstEarlierUpsert(
      boolean alternateValidation) {
    // ENTITY_URN is stored as the latest of its own set; the batch moves it to OTHER_VERSION_SET
    // with a low sortId, then raises the sortId past OTHER_LATEST_URN.
    allowEdit(ENTITY_URN, OWN_VERSION_SET_URN, OTHER_VERSION_SET_URN);
    List<BatchItem> batch =
        List.of(
            upsert(OTHER_VERSION_SET_URN, "v0", "aaa"),
            patch(ENTITY_URN, alternateValidation, false, patchOp("/sortId", "zzz")));
    Assert.assertFalse(validate(batch).isEmpty());
  }

  @Test(dataProvider = "patchItemBuilders")
  public void testUnresolvablePatchIsDenied(boolean alternateValidation) {
    // A template patch on an entity with no versionProperties cannot be applied.
    allowEdit(SECOND_ENTITY_URN, OTHER_VERSION_SET_URN, OTHER_LATEST_URN);
    BatchItem patch =
        patch(SECOND_ENTITY_URN, alternateValidation, false, patchOp("/sortId", "zzz"));
    Assert.assertFalse(validate(patch).isEmpty());
  }

  @Test
  public void testBatchReadsDoNotScaleWithItemCount() {
    allowEdit(ENTITY_URN, SECOND_ENTITY_URN, OTHER_VERSION_SET_URN);
    List<BatchItem> items =
        List.of(
            upsert(OTHER_VERSION_SET_URN, "v2", "ccc"),
            upsertFor(SECOND_ENTITY_URN, OTHER_VERSION_SET_URN, "v3", "ddd"));
    List<AspectValidationException> failures =
        validator
            .validateProposedAspectsWithAuth(
                OperationFingerprint.EMPTY, items, retrieverContext, session)
            .toList();
    // Both would demote the non-editable previous latest.
    Assert.assertEquals(failures.size(), 2);
    // Stored aspect, version set, previous latest: one lookup each for the whole batch.
    Mockito.verify(aspectRetriever, Mockito.times(3)).getLatestAspectObjects(any(), any(), any());
  }

  private void allowEdit(Urn... allowed) {
    Set<Urn> allowedUrns = Set.of(allowed);
    authUtil
        .when(() -> AuthUtil.isAuthorizedEntityUrns(eq(session), eq(ApiOperation.UPDATE), any()))
        .thenAnswer(
            invocation -> {
              Collection<Urn> urns = invocation.getArgument(2);
              return allowedUrns.containsAll(urns);
            });
  }

  private List<AspectValidationException> validate(BatchItem item) {
    return validate(List.of(item));
  }

  private List<AspectValidationException> validate(List<BatchItem> items) {
    return validator
        .validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY, items, retrieverContext, session)
        .toList();
  }

  private static GenericJsonPatch.PatchOp patchOp(String path, Object value) {
    GenericJsonPatch.PatchOp op = new GenericJsonPatch.PatchOp();
    op.setOp("add");
    op.setPath(path);
    op.setValue(value);
    return op;
  }

  private static BatchItem patch(
      Urn entity,
      boolean alternateValidation,
      boolean forceGenericPatch,
      GenericJsonPatch.PatchOp... ops) {
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(entity)
            .setEntityType(entity.getEntityType())
            .setAspectName(VERSION_PROPERTIES_ASPECT_NAME)
            .setChangeType(ChangeType.PATCH)
            .setAspect(
                GenericRecordUtils.serializePatch(
                    GenericJsonPatch.builder()
                        .patch(List.of(ops))
                        .forceGenericPatch(forceGenericPatch)
                        .build(),
                    new ObjectMapper()));
    return alternateValidation
        ? ProposedItem.builder()
            .build(mcp, AuditStampUtils.createDefaultAuditStamp(), TEST_REGISTRY)
        : PatchItemImpl.builder()
            .build(mcp, AuditStampUtils.createDefaultAuditStamp(), TEST_REGISTRY);
  }

  private static BatchItem upsert(Urn versionSet, String label, String sortId) {
    return upsertFor(ENTITY_URN, versionSet, label, sortId);
  }

  private static BatchItem upsertFor(Urn entity, Urn versionSet, String label, String sortId) {
    return TestMCP.ofOneUpsertItem(
            entity, versionProperties(versionSet, label, sortId), TEST_REGISTRY)
        .iterator()
        .next();
  }

  private static VersionProperties versionProperties(Urn versionSet, String label, String sortId) {
    return new VersionProperties()
        .setVersionSet(versionSet)
        .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
        .setVersion(new VersionTag().setVersionTag(label))
        .setSortId(sortId);
  }

  private static VersionSetProperties setProperties(Urn latest) {
    return new VersionSetProperties()
        .setLatest(latest)
        .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING);
  }
}
