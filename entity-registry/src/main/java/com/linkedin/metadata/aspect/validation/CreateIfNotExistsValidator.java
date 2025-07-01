package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/** Common implementation of checking for create if not exists semantics. */
@Setter
@Getter
@Accessors(chain = true)
public class CreateIfNotExistsValidator extends AspectPayloadValidator {
  public static final String FILTER_EXCEPTION_HEADER = "If-None-Match";
  public static final String FILTER_EXCEPTION_VALUE = "*";

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    // This logic relies on the fact that key aspects are either explicitly created (in the batch)
    // or the key aspect is auto generated as part of the default aspects and included
    // within a batch.
    // Meaning the presence of the key aspect indicates that the entity doesn't exist and CREATE
    // should be allowed
    Map<Urn, Set<ChangeMCP>> entityKeyMap =
        changeMCPs.stream()
            .filter(item -> item.getEntitySpec().getKeyAspectName().equals(item.getAspectName()))
            .collect(Collectors.groupingBy(ReadItem::getUrn, Collectors.toSet()));

    for (ChangeMCP createEntityItem :
        changeMCPs.stream()
            .filter(item -> ChangeType.CREATE_ENTITY.equals(item.getChangeType()))
            .collect(Collectors.toSet())) {
      // if the key aspect is missing in the batch, the entity exists and CREATE_ENTITY should be
      // denied or dropped
      if (!entityKeyMap.containsKey(createEntityItem.getUrn())) {
        if (isPrecondition(createEntityItem)) {
          exceptions.addException(
              AspectValidationException.forFilter(
                  createEntityItem, "Dropping write per precondition header If-None-Match: *"));
        } else {
          exceptions.addException(
              createEntityItem,
              "Cannot perform CREATE_ENTITY if not exists since the entity key already exists.");
        }
      }
    }

    for (ChangeMCP createItem :
        changeMCPs.stream()
            .filter(item -> ChangeType.CREATE.equals(item.getChangeType()))
            .collect(Collectors.toSet())) {
      // if a CREATE item has a previous value, should be denied or dropped
      if (createItem.getPreviousRecordTemplate() != null) {
        if (isPrecondition(createItem)) {
          exceptions.addException(
              AspectValidationException.forFilter(
                  createItem, "Dropping write per precondition header If-None-Match: *"));
        } else {
          exceptions.addException(
              createItem, "Cannot perform CREATE since the aspect already exists.");
        }
      }
    }

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  private static boolean isPrecondition(ChangeMCP item) {
    return item.getHeader(FILTER_EXCEPTION_HEADER)
        .map(FILTER_EXCEPTION_VALUE::equals)
        .orElse(false);
  }
}
