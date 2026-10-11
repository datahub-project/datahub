package com.linkedin.metadata.forms.validation;

import static com.linkedin.metadata.Constants.*;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class FormPromptValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  private static final String PROMPT_ID_FIELD = "promptId";
  private static final String PROMPT_ID_JSON_FIELD = "id";
  private static final String PROMPTS_PATH = "/prompts";

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validateFormInfoUpserts(mcpItems, retrieverContext);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @VisibleForTesting
  public static Stream<AspectValidationException> validateFormInfoUpserts(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (BatchItem mcpItem : mcpItems) {
      if (ChangeType.PATCH.equals(mcpItem.getChangeType()) && mcpItem instanceof MCPItem) {
        validatePatchItem((MCPItem) mcpItem, retrieverContext, exceptions);
        continue;
      }
      FormInfo formInfo = mcpItem.getAspect(FormInfo.class);
      if (formInfo != null) {
        validateFormInfo(mcpItem, formInfo, retrieverContext, exceptions);
      }
    }
    return exceptions.streamAllExceptions();
  }

  /**
   * A patch item carries only its delta, so validate the prompt IDs the patch itself writes: a
   * root/whole-array value gets the full check (in-form duplicates and cross-form uniqueness),
   * while individually added prompts are checked for cross-form uniqueness only — the prompts array
   * is keyed by id, so an in-form duplicate collapses on merge. Deeper sub-field operations do not
   * change a prompt's id and unparseable values are left to schema validation at merge time.
   */
  private static void validatePatchItem(
      MCPItem item, RetrieverContext retrieverContext, ValidationExceptionCollection exceptions) {
    List<String> addedPromptIds = new ArrayList<>();

    PatchOperationUtils.addAndReplaceValues(item)
        .forEach(
            op -> {
              final String path = op.getFirst();
              final JsonValue value = op.getSecond();
              try {
                if (path.isEmpty() || "/".equals(path)) {
                  validateFormInfo(
                      item,
                      RecordUtils.toRecordTemplate(FormInfo.class, value.toString()),
                      retrieverContext,
                      exceptions);
                } else if (path.equals(PROMPTS_PATH)) {
                  validateFormInfo(
                      item,
                      RecordUtils.toRecordTemplate(FormInfo.class, "{\"prompts\":" + value + "}"),
                      retrieverContext,
                      exceptions);
                } else if (isPromptElementPath(path)
                    && value.getValueType() == JsonValue.ValueType.OBJECT) {
                  JsonObject prompt = value.asJsonObject();
                  String id = prompt.getString(PROMPT_ID_JSON_FIELD, null);
                  if (id != null && !id.isEmpty()) {
                    addedPromptIds.add(id);
                  }
                }
              } catch (RuntimeException e) {
                // unparseable delta — schema validation rejects it at merge time
              }
            });

    if (!addedPromptIds.isEmpty()) {
      checkCrossFormConflicts(item, addedPromptIds, retrieverContext, exceptions);
    }
  }

  /** True for {@code /prompts/<key>} exactly — deeper sub-field paths do not change the id. */
  private static boolean isPromptElementPath(String path) {
    if (!path.startsWith(PROMPTS_PATH + "/")) {
      return false;
    }
    String remainder = path.substring(PROMPTS_PATH.length() + 1);
    if (remainder.endsWith("/")) {
      remainder = remainder.substring(0, remainder.length() - 1);
    }
    return !remainder.isEmpty() && !remainder.contains("/");
  }

  private static void validateFormInfo(
      BatchItem mcpItem,
      FormInfo formInfo,
      RetrieverContext retrieverContext,
      ValidationExceptionCollection exceptions) {
    List<String> promptIds =
        formInfo.getPrompts().stream().map(FormPrompt::getId).collect(Collectors.toList());
    Set<String> uniquePromptIds = new HashSet<>(promptIds);
    if (promptIds.size() != uniquePromptIds.size()) {
      exceptions.addException(
          mcpItem,
          String.format(
              "Prompt IDs must be unique, there are duplicate prompt IDs given: %s", promptIds));
    }
    checkCrossFormConflicts(mcpItem, promptIds, retrieverContext, exceptions);
  }

  private static void checkCrossFormConflicts(
      BatchItem mcpItem,
      List<String> promptIds,
      RetrieverContext retrieverContext,
      ValidationExceptionCollection exceptions) {
    // Search to find other forms with prompts with the same ID as any of this form's prompts
    SearchRetriever searchRetriever = retrieverContext.getSearchRetriever();
    ScrollResult scrollResult =
        searchRetriever.scroll(
            Collections.singletonList(FORM_ENTITY_NAME),
            FilterUtils.createValuesFilter(PROMPT_ID_FIELD, promptIds),
            null,
            10,
            new ArrayList<>(),
            SearchRetriever.RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS);

    if (CollectionUtils.isNotEmpty(scrollResult.getEntities())) {
      Urn urnFromMcp = mcpItem.getUrn();
      List<SearchEntity> otherEntities =
          scrollResult.getEntities().stream()
              .filter(e -> !e.getEntity().equals(urnFromMcp))
              .collect(Collectors.toList());
      if (otherEntities.size() > 0) {
        exceptions.addException(
            mcpItem,
            "Cannot have duplicate prompt IDs across any form, all prompt IDs must be globally unique. Form urns with prompt IDs matching given prompt IDs:"
                + otherEntities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()));
      }
    }
  }
}
