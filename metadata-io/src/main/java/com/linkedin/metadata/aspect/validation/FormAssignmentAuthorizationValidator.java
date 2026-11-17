package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORMS_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.Forms;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Requires the platform-level {@code MANAGE_DOCUMENTATION_FORMS} privilege to change which forms
 * are assigned to an asset, regardless of which API the write arrives through.
 *
 * <p>Two aspects are covered. A {@code dynamicFormAssignment} write on a form always requires the
 * privilege, since it drives system-side assignment across every matching asset. A {@code forms}
 * write on an asset requires it only when the set of associated form URNs (incomplete plus
 * completed) changes. Prompt completion and verification also rewrite {@code forms} with the acting
 * user's context, but they never add or remove a form association, so those writes pass without the
 * privilege. A PATCH that cannot be resolved fails closed and requires the privilege.
 */
@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class FormAssignmentAuthorizationValidator extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    final List<BatchItem> formsItems =
        items.stream()
            .filter(item -> FORMS_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());
    final List<BatchItem> dynamicItems =
        items.stream()
            .filter(item -> DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME.equals(item.getAspectName()))
            .collect(Collectors.toList());

    // Lazily evaluated: the privilege is platform-level, so one check covers the whole batch.
    Boolean canManageForms = null;
    final List<AspectValidationException> failures = new ArrayList<>();

    for (BatchItem item : dynamicItems) {
      if (canManageForms == null) {
        canManageForms = EntityAspectAuthorizationUtils.isAuthorizedToManageForms(session);
      }
      if (!canManageForms) {
        failures.add(
            authFailure(
                item, "Unauthorized to modify dynamic form assignment on form: " + item.getUrn()));
      }
    }

    if (!formsItems.isEmpty()) {
      final Map<Urn, Map<String, Aspect>> current =
          AspectRetriever.getLatestAspectObjectsAcrossEntityTypes(
              retrieverContext.getAspectRetriever(),
              operationContext,
              formsItems.stream().map(BatchItem::getUrn).collect(Collectors.toSet()),
              Set.of(FORMS_ASPECT_NAME));

      for (BatchItem item : formsItems) {
        final Forms before = currentForms(current, item.getUrn());
        final Forms after = resolveProposed(item, before);
        if (after != null && assignedFormUrns(before).equals(assignedFormUrns(after))) {
          continue;
        }
        if (canManageForms == null) {
          canManageForms = EntityAspectAuthorizationUtils.isAuthorizedToManageForms(session);
        }
        if (!canManageForms) {
          failures.add(
              authFailure(
                  item, "Unauthorized to change form assignments on entity: " + item.getUrn()));
        }
      }
    }

    return failures;
  }

  @Nonnull
  private static Set<Urn> assignedFormUrns(@Nullable Forms forms) {
    final Set<Urn> urns = new HashSet<>();
    if (forms == null) {
      return urns;
    }
    if (forms.hasIncompleteForms()) {
      forms.getIncompleteForms().stream().map(FormAssociation::getUrn).forEach(urns::add);
    }
    if (forms.hasCompletedForms()) {
      forms.getCompletedForms().stream().map(FormAssociation::getUrn).forEach(urns::add);
    }
    return urns;
  }

  @Nullable
  private static Forms currentForms(
      @Nonnull Map<Urn, Map<String, Aspect>> currentAspects, @Nonnull Urn urn) {
    final Aspect aspect = currentAspects.getOrDefault(urn, Map.of()).get(FORMS_ASPECT_NAME);
    return aspect == null ? null : RecordUtils.toRecordTemplate(Forms.class, aspect.data());
  }

  /**
   * The proposed {@code forms} value, or null when it cannot be determined so the caller fails
   * closed. For PATCH the full JsonPatch is applied on top of the current value.
   */
  @Nullable
  private static Forms resolveProposed(@Nonnull BatchItem item, @Nullable Forms current) {
    if (!ChangeType.PATCH.equals(item.getChangeType()) || !(item instanceof MCPItem)) {
      return item.getAspect(Forms.class);
    }
    final JsonPatch patch = PatchOperationUtils.resolveJsonPatch((MCPItem) item);
    if (patch == null) {
      return null;
    }
    final Forms base = current != null ? current : new Forms(new DataMap());
    try (JsonReader reader = Json.createReader(new StringReader(RecordUtils.toJsonString(base)))) {
      final JsonObject patched = patch.apply(reader.readObject());
      return RecordUtils.toRecordTemplate(Forms.class, patched.toString());
    } catch (RuntimeException e) {
      log.warn(
          "Unable to apply forms PATCH for authorization check on {}; failing closed: {}",
          item.getUrn(),
          e.toString());
      return null;
    }
  }
}
