package com.linkedin.metadata.models.registry.template;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonpatch.Patch;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Holds connection between aspect specs and their templates and drives the generation from
 * templates
 */
public class AspectTemplateEngine {

  public static final Set<String> SUPPORTED_TEMPLATES =
      Stream.of(
              DATASET_PROPERTIES_ASPECT_NAME,
              EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
              GLOBAL_TAGS_ASPECT_NAME,
              GLOSSARY_TERMS_ASPECT_NAME,
              OWNERSHIP_ASPECT_NAME,
              UPSTREAM_LINEAGE_ASPECT_NAME,
              DATA_FLOW_INFO_ASPECT_NAME,
              DATA_JOB_INFO_ASPECT_NAME,
              DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
              DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)
          .collect(Collectors.toSet());

  private final Map<String, Template<? extends RecordTemplate>> _aspectTemplateMap;

  public AspectTemplateEngine() {
    _aspectTemplateMap = new HashMap<>();
  }

  public AspectTemplateEngine(Map<String, Template<? extends RecordTemplate>> aspectTemplateMap) {
    _aspectTemplateMap = aspectTemplateMap;
  }

  @Nullable
  public RecordTemplate getDefaultTemplate(String aspectSpecName) {
    return _aspectTemplateMap.containsKey(aspectSpecName)
        ? _aspectTemplateMap.get(aspectSpecName).getDefault()
        : null;
  }

  /**
   * Applies a json patch to a record, optionally merging array fields as necessary
   *
   * @param recordTemplate original template to be updated
   * @param jsonPatch patch to apply
   * @param aspectSpec aspectSpec of the template
   * @return a {@link RecordTemplate} with the patch applied
   * @throws JsonProcessingException if there is an issue with processing the record template's json
   * @throws JsonPatchException if there is an issue with applying the json patch
   */
  @Nonnull
  public <T extends RecordTemplate> RecordTemplate applyPatch(
      RecordTemplate recordTemplate, Patch jsonPatch, AspectSpec aspectSpec)
      throws JsonProcessingException, JsonPatchException {

    Template<T> template = getTemplate(aspectSpec);
    return template.applyPatch(recordTemplate, jsonPatch);
  }

  // Get around lack of generics on AspectSpec data template class
  private <T extends RecordTemplate> Template<T> getTemplate(AspectSpec aspectSpec) {
    return (Template<T>) _aspectTemplateMap.getOrDefault(aspectSpec.getName(), null);
  }
}
