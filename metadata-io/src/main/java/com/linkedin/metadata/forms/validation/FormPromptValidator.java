package com.linkedin.metadata.forms.validation;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
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
import org.apache.commons.collections.CollectionUtils;

@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class FormPromptValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  private static final String PROMPT_ID_FIELD = "promptId";

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validateFormInfoUpserts(mcpItems, retrieverContext);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @VisibleForTesting
  public static Stream<AspectValidationException> validateFormInfoUpserts(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (BatchItem mcpItem : mcpItems) {
      FormInfo formInfo = mcpItem.getAspect(FormInfo.class);
      if (formInfo != null) {
        List<String> promptIds =
            formInfo.getPrompts().stream().map(FormPrompt::getId).collect(Collectors.toList());
        Set<String> uniquePromptIds = new HashSet<>(promptIds);
        if (promptIds.size() != uniquePromptIds.size()) {
          exceptions.addException(
              mcpItem,
              String.format(
                  "Prompt IDs must be unique, there are duplicate prompt IDs given: %s",
                  promptIds));
        }
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
          if (scrollResult.getEntities().size() > 0) {
            exceptions.addException(
                mcpItem,
                "Cannot have duplicate prompt IDs across any form, all prompt IDs must be globally unique. Form urns with prompt IDs matching given prompt IDs:"
                    + scrollResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList()));
          }
        }
      }
    }
    return exceptions.streamAllExceptions();
  }
}
