package com.linkedin.metadata.entity.ebean;

import com.linkedin.metadata.aspect.SystemAspect;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Identity mark for a semantic no-op. Kept off {@link EbeanSystemAspect}'s constructor so the
 * persisted row shape does not change and existing all-args construction stays valid.
 */
final class SemanticNoOpMarks {
  private static final Map<SystemAspect, Boolean> MARKS =
      Collections.synchronizedMap(new WeakHashMap<>());

  private SemanticNoOpMarks() {}

  static void set(SystemAspect aspect, boolean semanticNoOp) {
    if (semanticNoOp) {
      MARKS.put(aspect, Boolean.TRUE);
    } else {
      MARKS.remove(aspect);
    }
  }

  static boolean isSet(SystemAspect aspect) {
    return Boolean.TRUE.equals(MARKS.get(aspect));
  }
}
