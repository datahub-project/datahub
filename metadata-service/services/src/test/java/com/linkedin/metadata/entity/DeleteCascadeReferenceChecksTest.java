package com.linkedin.metadata.entity;

import static com.linkedin.metadata.entity.DeleteCascadeReferenceChecks.isLiveFileReference;
import static com.linkedin.metadata.entity.DeleteCascadeReferenceChecks.searchAspectReferences;
import static com.linkedin.metadata.entity.DeleteCascadeReferenceChecks.versionOf;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormVerificationAssociation;
import com.linkedin.common.FormVerificationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.validation.ConditionalWriteValidator;
import com.linkedin.mxe.SystemMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

public class DeleteCascadeReferenceChecksTest {

  private static final Urn FORM = UrnUtils.getUrn("urn:li:form:checks-form");
  private static final Urn PROPERTY = UrnUtils.getUrn("urn:li:structuredProperty:checks.property");
  private static final Urn FILE = UrnUtils.getUrn("urn:li:dataHubFile:checks-file");

  @Test
  public void versionFollowsTheConditionalWriteValidatorRule() {
    assertEquals(versionOf(envelope(0L, "12")), 12L);
    assertEquals(versionOf(envelope(0L, "007")), 7L);
    // No numeric systemMetadata.version: the validator falls back to max(1, row version).
    assertEquals(versionOf(envelope(0L, "not-a-number")), 1L);
    assertEquals(versionOf(envelope(0L, null)), 1L);
    assertEquals(versionOf(envelope(5L, null)), 5L);
  }

  /**
   * One version rule: the envelope the cascade reads yields exactly what resolveAspectVersion
   * yields for the row behind it (toEnvelopedAspects copies the row version and system metadata).
   */
  @Test
  public void versionOfAnEnvelopeEqualsResolveAspectVersionOfItsRow() {
    // A non-numeric systemMetadata.version is left out: SystemAspect parses it leniently in some
    // builds and throws in others; versionFollowsTheConditionalWriteValidatorRule pins our side.
    final Object[][] rows = {{0L, "12"}, {0L, "007"}, {0L, null}, {5L, null}, {5L, "2"}};
    for (Object[] row : rows) {
      final long rowVersion = (Long) row[0];
      final String metadataVersion = (String) row[1];
      final SystemAspect systemAspect = mock(SystemAspect.class, CALLS_REAL_METHODS);
      doReturn(rowVersion).when(systemAspect).getVersion();
      doReturn(metadataVersion == null ? null : new SystemMetadata().setVersion(metadataVersion))
          .when(systemAspect)
          .getSystemMetadata();

      assertEquals(
          versionOf(envelope(rowVersion, metadataVersion)),
          ConditionalWriteValidator.resolveAspectVersion(systemAspect),
          "row " + Arrays.toString(row));
    }
  }

  @Test
  public void formsAndFormInfoReferencesAreFoundInEveryField() {
    assertTrue(searchAspectReferences("forms", aspect(forms(FORM, null, null)), FORM));
    assertTrue(searchAspectReferences("forms", aspect(forms(null, FORM, null)), FORM));
    assertTrue(searchAspectReferences("forms", aspect(forms(null, null, FORM)), FORM));
    assertFalse(searchAspectReferences("forms", aspect(forms(null, null, null)), FORM));

    final FormInfo withPrompt =
        new FormInfo()
            .setName("form")
            .setPrompts(
                new FormPromptArray(
                    List.of(
                        new FormPrompt()
                            .setId("prompt")
                            .setTitle("prompt")
                            .setType(FormPromptType.STRUCTURED_PROPERTY)
                            .setStructuredPropertyParams(
                                new StructuredPropertyParams().setUrn(PROPERTY)))));
    assertTrue(searchAspectReferences("formInfo", aspect(withPrompt), PROPERTY));
    assertFalse(
        searchAspectReferences(
            "formInfo",
            aspect(new FormInfo().setName("form").setPrompts(new FormPromptArray())),
            PROPERTY));
  }

  @Test
  public void fileIsALiveReferenceUntilSoftDeleted() {
    assertFalse(isLiveFileReference(null));
    // The index entry outlived the file's info: nothing left to clean.
    assertFalse(isLiveFileReference(file(false, null)));
    assertTrue(isLiveFileReference(file(true, null)));
    assertTrue(isLiveFileReference(file(true, false)));
    assertFalse(isLiveFileReference(file(true, true)));
  }

  private static EnvelopedAspect envelope(long rowVersion, @Nullable String systemMetadataVersion) {
    final EnvelopedAspect envelope =
        new EnvelopedAspect().setName("domains").setVersion(rowVersion).setValue(new Aspect());
    if (systemMetadataVersion != null) {
      envelope.setSystemMetadata(new SystemMetadata().setVersion(systemMetadataVersion));
    }
    return envelope;
  }

  private static Aspect aspect(com.linkedin.data.template.RecordTemplate record) {
    return new Aspect(record.data());
  }

  private static Forms forms(
      @Nullable Urn incomplete, @Nullable Urn completed, @Nullable Urn verified) {
    return new Forms()
        .setIncompleteForms(
            new FormAssociationArray(
                incomplete == null ? List.of() : List.of(new FormAssociation().setUrn(incomplete))))
        .setCompletedForms(
            new FormAssociationArray(
                completed == null ? List.of() : List.of(new FormAssociation().setUrn(completed))))
        .setVerifications(
            new FormVerificationAssociationArray(
                verified == null
                    ? List.of()
                    : List.of(new FormVerificationAssociation().setForm(verified))));
  }

  private static EntityResponse file(boolean hasInfo, @Nullable Boolean removed) {
    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    if (hasInfo) {
      aspects.put(
          Constants.DATAHUB_FILE_INFO_ASPECT_NAME,
          new EnvelopedAspect()
              .setName(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)
              .setValue(new Aspect()));
    }
    if (removed != null) {
      aspects.put(
          Constants.STATUS_ASPECT_NAME,
          new EnvelopedAspect()
              .setName(Constants.STATUS_ASPECT_NAME)
              .setValue(new Aspect(new Status().setRemoved(removed).data())));
    }
    return new EntityResponse()
        .setUrn(FILE)
        .setEntityName(Constants.DATAHUB_FILE_ENTITY_NAME)
        .setAspects(new EnvelopedAspectMap(aspects));
  }
}
