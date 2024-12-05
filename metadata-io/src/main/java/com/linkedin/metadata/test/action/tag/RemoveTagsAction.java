package com.linkedin.metadata.test.action.tag;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.action.ActionUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.TagServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.UrnValuesAction;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveTagsAction extends UrnValuesAction {

  private final TagServiceAsync tagService;

  @Override
  public ActionType getActionType() {
    return ActionType.REMOVE_TAGS;
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    List<String> tagUrnStrs = params.getParams().get(VALUES_PARAM);
    List<Urn> tagUrns = tagUrnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrns : entityTypesToUrns.entrySet()) {
      applyInternal(opContext, tagUrns, entityTypeToUrns.getValue());
    }
  }

  @Override
  protected Set<String> validValueEntityTypes() {
    return Set.of(TAG_ENTITY_NAME);
  }

  private void applyInternal(
      @Nonnull OperationContext opContext, List<Urn> tagUrns, List<Urn> urns) {
    if (!urns.isEmpty()) {
      this.tagService.batchRemoveTags(
          opContext, tagUrns, getResourceReferences(urns), METADATA_TESTS_SOURCE);
    }
  }
}
