package com.linkedin.metadata.test.action.tag;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.TagService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.ActionType;
import com.linkedin.metadata.test.action.api.ValuesAction;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.action.ActionUtils.*;


@Slf4j
@RequiredArgsConstructor
public class AddTagsAction extends ValuesAction {

  private final TagService tagService;

  @Override
  public ActionType getActionType() {
    return ActionType.ADD_TAGS;
  }

  @Override
  public void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException {
    List<String> tagUrnStrs = params.getParams().get(VALUES_PARAM);
    List<Urn> tagUrns = tagUrnStrs.stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());

    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrns : entityTypesToUrns.entrySet()) {
      applyInternal(tagUrns, entityTypeToUrns.getValue());
    }
  }

  private void applyInternal(List<Urn> tagUrns, List<Urn> urns) {
    if (!urns.isEmpty()) {
      this.tagService.batchAddTags(tagUrns, getResourceReferences(urns), METADATA_TESTS_SOURCE);
    }
  }
}