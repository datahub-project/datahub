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
import org.testng.annotations.Test;

public class VersionPropertiesAuthorizationValidatorTest {
  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  // Entity the caller may edit.
  private static final Urn ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,entity,PROD)");
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

    MockAspectRetriever aspectRetriever =
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

  @Test
  public void testPatchMovingToAnotherVersionSetIsChecked() {
    allowEdit(ENTITY_URN);
    GenericJsonPatch.PatchOp op = new GenericJsonPatch.PatchOp();
    op.setOp("add");
    op.setPath("/versionSet");
    op.setValue(OTHER_VERSION_SET_URN.toString());
    GenericJsonPatch.PatchOp sortOp = new GenericJsonPatch.PatchOp();
    sortOp.setOp("add");
    sortOp.setPath("/sortId");
    sortOp.setValue("ccc");
    MetadataChangeProposal mcp =
        new MetadataChangeProposal()
            .setEntityUrn(ENTITY_URN)
            .setEntityType(ENTITY_URN.getEntityType())
            .setAspectName(VERSION_PROPERTIES_ASPECT_NAME)
            .setChangeType(ChangeType.PATCH)
            .setAspect(
                GenericRecordUtils.serializePatch(
                    GenericJsonPatch.builder().patch(List.of(op, sortOp)).build(),
                    new ObjectMapper()));
    BatchItem patch =
        ProposedItem.builder().build(mcp, AuditStampUtils.createDefaultAuditStamp(), TEST_REGISTRY);
    Assert.assertFalse(validate(patch).isEmpty());
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
    return validator
        .validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY, List.of(item), retrieverContext, session)
        .toList();
  }

  private static BatchItem upsert(Urn versionSet, String label, String sortId) {
    return TestMCP.ofOneUpsertItem(
            ENTITY_URN, versionProperties(versionSet, label, sortId), TEST_REGISTRY)
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
