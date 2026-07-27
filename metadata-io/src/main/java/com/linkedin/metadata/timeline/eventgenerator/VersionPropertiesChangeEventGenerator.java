package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Timeline event generator for the {@code versionProperties} aspect.
 *
 * <p>Emits {@link ChangeCategory#VERSIONING} events so that version creation is visible in the
 * {@code getTimeline} API and in the DataHub UI change history sidebar. Currently covers two
 * events: a version being created (first write of {@code versionProperties}) and a {@code
 * versionTag} change. Lifecycle-stage transitions (DRAFT → PUBLISHED etc.) live on a separate
 * aspect and are NOT emitted by this generator.
 *
 * <p>Each emitted event carries structured {@code parameters} so the UI can render a rich version
 * milestone without parsing the description string. Keys: {@code versionTag}, {@code comment},
 * {@code versionSetUrn}, {@code isLatest}.
 *
 * <p>Test with: {@code datahub timeline --urn <glossaryTermUrn> -c versioning}
 */
@Slf4j
public class VersionPropertiesChangeEventGenerator
    extends EntityChangeEventGenerator<VersionProperties> {

  private static final String VERSION_CREATED_FORMAT = "Version '%s' created for '%s'";
  private static final String VERSION_CREATED_WITH_COMMENT_FORMAT =
      "Version '%s' created for '%s' — %s";
  private static final String VERSION_TAG_CHANGED_FORMAT =
      "Version tag of '%s' changed from '%s' to '%s'";

  // ── getSemanticDiff (deprecated path, still called by TimelineServiceImpl) ──

  @Override
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory element,
      JsonPatch rawDiff,
      boolean rawDiffsRequested) {
    if (!currentValue.getAspect().equals(VERSION_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + VERSION_PROPERTIES_ASPECT_NAME);
    }

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.VERSIONING) {
      VersionProperties prev = getVersionPropertiesFromAspect(previousValue);
      VersionProperties curr = getVersionPropertiesFromAspect(currentValue);
      changeEvents.addAll(computeDiffs(prev, curr, currentValue.getUrn(), null));
    }

    SemanticChangeType highestSemVer = SemanticChangeType.NONE;
    ChangeEvent highestEvent =
        changeEvents.stream().max(Comparator.comparing(ChangeEvent::getSemVerChange)).orElse(null);
    if (highestEvent != null) {
      highestSemVer = highestEvent.getSemVerChange();
    }

    return ChangeTransaction.builder()
        .semVerChange(highestSemVer)
        .changeEvents(changeEvents)
        .timestamp(currentValue.getCreatedOn().getTime())
        .rawDiff(rawDiffsRequested ? rawDiff : null)
        .actor(currentValue.getCreatedBy())
        .build();
  }

  // ── getChangeEvents (preferred path) ──────────────────────────────────────

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<VersionProperties> from,
      @Nonnull Aspect<VersionProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  // ── Core diff logic ───────────────────────────────────────────────────────

  private static List<ChangeEvent> computeDiffs(
      @Nullable VersionProperties prev,
      @Nullable VersionProperties curr,
      @Nonnull String entityUrn,
      @Nullable AuditStamp auditStamp) {

    List<ChangeEvent> events = new ArrayList<>();
    if (curr == null) {
      return events;
    }

    String currTag = curr.hasVersion() ? curr.getVersion().getVersionTag() : null;
    String prevTag = (prev != null && prev.hasVersion()) ? prev.getVersion().getVersionTag() : null;

    if (prev == null) {
      events.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.VERSIONING)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(buildCreatedDescription(entityUrn, currTag, curr))
              .parameters(buildParameters(curr, currTag))
              .auditStamp(auditStamp)
              .build());
    } else if (currTag != null && !currTag.equals(prevTag)) {
      Map<String, Object> params = buildParameters(curr, currTag);
      if (prevTag != null) {
        params.put("previousVersionTag", prevTag);
      }
      events.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.VERSIONING)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.PATCH)
              .description(String.format(VERSION_TAG_CHANGED_FORMAT, entityUrn, prevTag, currTag))
              .parameters(params)
              .auditStamp(auditStamp)
              .build());
    }

    return events;
  }

  private static Map<String, Object> buildParameters(VersionProperties vp, @Nullable String tag) {
    Map<String, Object> params = new HashMap<>();
    if (tag != null) {
      params.put("versionTag", tag);
    }
    if (vp.hasComment() && vp.getComment() != null && !vp.getComment().isBlank()) {
      params.put("comment", vp.getComment());
    }
    if (vp.hasVersionSet() && vp.getVersionSet() != null) {
      params.put("versionSetUrn", vp.getVersionSet().toString());
    }
    params.put("isLatest", Boolean.toString(vp.hasIsLatest() && vp.isIsLatest()));
    return params;
  }

  private static String buildCreatedDescription(
      String entityUrn, @Nullable String versionTag, VersionProperties vp) {
    String tag = versionTag != null ? versionTag : "unknown";
    String comment = vp.hasComment() ? vp.getComment() : null;
    if (comment != null && !comment.isBlank()) {
      return String.format(VERSION_CREATED_WITH_COMMENT_FORMAT, tag, entityUrn, comment);
    }
    return String.format(VERSION_CREATED_FORMAT, tag, entityUrn);
  }

  @Nullable
  private static VersionProperties getVersionPropertiesFromAspect(
      @Nullable EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(VersionProperties.class, entityAspect.getMetadata());
    }
    return null;
  }
}
