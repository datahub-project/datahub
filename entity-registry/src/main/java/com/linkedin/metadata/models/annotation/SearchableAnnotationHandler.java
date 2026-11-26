package com.linkedin.metadata.models.annotation;

import com.linkedin.data.schema.annotation.PegasusSchemaAnnotationHandlerImpl;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler.ResolutionMetaData;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler.ResolutionResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Resolves {@code @Searchable} the way Pegasus does, except a path override that only sets {@code
 * v3.degree}, {@code v3.boostScore}, and {@code v3.queryByDefault} is merged onto the annotation
 * already in effect. A full override, one that sets {@code fieldType}, still replaces that
 * annotation. The top-level {@code queryByDefault} stays the Search V2 flag.
 */
public class SearchableAnnotationHandler extends PegasusSchemaAnnotationHandlerImpl {

  private static final Set<String> PATCH_KEYS = Set.of("v3");
  private static final Set<String> PATCH_V3_KEYS = Set.of("degree", "boostScore", "queryByDefault");

  public SearchableAnnotationHandler() {
    super(SearchableAnnotation.ANNOTATION_NAME);
  }

  @Override
  public ResolutionResult resolve(
      List<Pair<String, Object>> annotations, ResolutionMetaData resolutionMetaData) {
    if (annotations == null
        || annotations.size() < 2
        || !isDegreePatch(annotations.get(0).getRight())) {
      return super.resolve(annotations, resolutionMetaData);
    }
    Object base = null;
    for (int i = 1; i < annotations.size(); i++) {
      Object candidate = annotations.get(i).getRight();
      if (!isDegreePatch(candidate)) {
        base = candidate;
        break;
      }
    }
    if (!(base instanceof Map<?, ?> baseMap)) {
      return super.resolve(annotations, resolutionMetaData);
    }
    Map<String, Object> merged = new HashMap<>();
    baseMap.forEach((key, value) -> merged.put(String.valueOf(key), value));
    Map<String, Object> v3 = new HashMap<>();
    if (merged.get("v3") instanceof Map<?, ?> baseV3) {
      baseV3.forEach((key, value) -> v3.put(String.valueOf(key), value));
    }
    Map<?, ?> patch = (Map<?, ?>) annotations.get(0).getRight();
    Map<?, ?> patchV3 = (Map<?, ?>) patch.get("v3");
    if (patchV3.containsKey("degree")) {
      v3.put("degree", patchV3.get("degree"));
    }
    if (patchV3.containsKey("boostScore")) {
      v3.put("boostScore", patchV3.get("boostScore"));
    }
    if (patchV3.containsKey("queryByDefault")) {
      v3.put("queryByDefault", patchV3.get("queryByDefault"));
    }
    merged.put("v3", v3);

    ResolutionResult result = new ResolutionResult();
    Map<String, Object> resolved = new HashMap<>();
    resolved.put(SearchableAnnotation.ANNOTATION_NAME, merged);
    result.setResolvedResult(resolved);
    return result;
  }

  /**
   * An override that sets the degree, the V3 score, {@code v3.queryByDefault}, or a combination,
   * and nothing else. All three sit under {@code v3}, so the patch does not change the Search V2
   * flag.
   */
  static boolean isDegreePatch(Object annotation) {
    if (!(annotation instanceof Map<?, ?> map)
        || map.isEmpty()
        || !PATCH_KEYS.containsAll(map.keySet())
        || !map.containsKey("v3")) {
      return false;
    }
    Object v3 = map.get("v3");
    if (!(v3 instanceof Map<?, ?> v3Map) || v3Map.isEmpty()) {
      return false;
    }
    return PATCH_V3_KEYS.containsAll(v3Map.keySet());
  }
}
