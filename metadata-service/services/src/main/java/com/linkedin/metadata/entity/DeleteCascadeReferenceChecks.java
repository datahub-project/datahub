package com.linkedin.metadata.entity;

import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;

import com.linkedin.common.Forms;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Pure checks used by the delete cascade. */
final class DeleteCascadeReferenceChecks {

  private DeleteCascadeReferenceChecks() {}

  /**
   * The version of the row behind {@code current}, by the single version rule ({@code
   * ConditionalWriteValidator.resolveAspectVersion}): a numeric systemMetadata.version, else max(1,
   * row version). EntityService returns envelopes, not rows; {@code
   * EntityAspect.EntitySystemAspect#toEnvelopedAspects} copies the row's version and system
   * metadata into the envelope, so both give the same number. Used as the If-Version-Match value of
   * an upsert and as the ceiling of {@code EntityService#deleteAspectUpToVersion}.
   */
  static long versionOf(@Nonnull EnvelopedAspect current) {
    if (current.hasSystemMetadata() && current.getSystemMetadata().hasVersion()) {
      try {
        return Long.parseLong(current.getSystemMetadata().getVersion());
      } catch (NumberFormatException e) {
        // SystemAspect treats a non-numeric version as absent: fall through to the row version.
      }
    }
    return Math.max(1L, current.hasVersion() ? current.getVersion() : 0L);
  }

  @Nonnull
  static StringMap ifVersionMatch(long version) {
    return new StringMap(Map.of(HTTP_HEADER_IF_VERSION_MATCH, String.valueOf(version)));
  }

  /** Whether a form-related aspect found by the search-reference scan still points at the urn. */
  static boolean searchAspectReferences(
      @Nonnull String aspectName, @Nonnull Aspect value, @Nonnull Urn deletedUrn) {
    if (Constants.FORMS_ASPECT_NAME.equals(aspectName)) {
      final Forms forms = new Forms(value.data());
      return (forms.hasIncompleteForms()
              && forms.getIncompleteForms().stream()
                  .anyMatch(form -> deletedUrn.equals(form.getUrn())))
          || (forms.hasCompletedForms()
              && forms.getCompletedForms().stream()
                  .anyMatch(form -> deletedUrn.equals(form.getUrn())))
          || forms.getVerifications().stream()
              .anyMatch(verification -> deletedUrn.equals(verification.getForm()));
    }
    if (Constants.FORM_INFO_ASPECT_NAME.equals(aspectName)) {
      return new FormInfo(value.data())
          .getPrompts().stream()
              .anyMatch(
                  prompt ->
                      prompt.getStructuredPropertyParams() != null
                          && deletedUrn.equals(prompt.getStructuredPropertyParams().getUrn()));
    }
    return false;
  }

  /** A file entity references its asset until it is soft-deleted, or until its info is gone. */
  static boolean isLiveFileReference(@Nullable EntityResponse file) {
    if (file == null || !file.getAspects().containsKey(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)) {
      return false;
    }
    final EnvelopedAspect status = file.getAspects().get(Constants.STATUS_ASPECT_NAME);
    return status == null || !Boolean.TRUE.equals(new Status(status.getValue().data()).isRemoved());
  }
}
