package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

/**
 * The embedding profiles a V3 reindex leaves behind. A model removed from {@code
 * semanticSearch.models} is mapped on the current index and not on the target. {@code _reindex}
 * copies {@code _source}, so without this the retired vectors move into the new index unmapped,
 * where nothing can select or remove them. A script runs on the copy because the Elasticsearch 8
 * client's reindex source cannot express an exclude.
 */
final class RetiredEmbeddingProfiles {

  private static final String RETIRED_PARAM = "retired";

  private static final String REMOVE_SCRIPT =
      "def embeddings = ctx._source.get('"
          + SemanticEmbeddingMappings.EMBEDDINGS_FIELD
          + "'); if (embeddings instanceof Map) { for (def key : params."
          + RETIRED_PARAM
          + ") { embeddings.remove(key); } }";

  private RetiredEmbeddingProfiles() {}

  /**
   * Profile keys the source index maps under {@code embeddings} and the destination does not. A
   * destination with no {@code embeddings} object reports every source profile. {@code
   * ESIndexBuilder} keeps those profiles when semantic search is off, and retires them when it is
   * on and every model was removed.
   */
  @Nonnull
  static Set<String> retiredKeys(
      @Nonnull Map<String, Object> sourceMappings, @Nonnull Map<String, Object> destMappings) {
    Set<String> retired = new TreeSet<>(profileKeys(sourceMappings));
    retired.removeAll(profileKeys(destMappings));
    return retired;
  }

  /** True when {@code properties.embeddings} is an object, including one with no profile keys. */
  static boolean declaresEmbeddings(@Nonnull Map<String, Object> mappings) {
    return mappings.get("properties") instanceof Map<?, ?> fields
        && fields.get(SemanticEmbeddingMappings.EMBEDDINGS_FIELD) instanceof Map<?, ?>;
  }

  /** The reindex script that drops the retired keys, or null when there are none. */
  @Nullable
  static Script removalScript(@Nonnull Set<String> retiredKeys) {
    if (retiredKeys.isEmpty()) {
      return null;
    }
    return new Script(
        ScriptType.INLINE,
        Script.DEFAULT_SCRIPT_LANG,
        REMOVE_SCRIPT,
        Map.of(RETIRED_PARAM, List.copyOf(retiredKeys)));
  }

  @Nonnull
  private static Set<String> profileKeys(@Nonnull Map<String, Object> mappings) {
    if (!(mappings.get("properties") instanceof Map<?, ?> fields)
        || !(fields.get(SemanticEmbeddingMappings.EMBEDDINGS_FIELD) instanceof Map<?, ?> embeddings)
        || !(embeddings.get("properties") instanceof Map<?, ?> profiles)) {
      return Set.of();
    }
    Set<String> keys = new TreeSet<>();
    profiles.keySet().forEach(key -> keys.add(String.valueOf(key)));
    return keys;
  }
}
