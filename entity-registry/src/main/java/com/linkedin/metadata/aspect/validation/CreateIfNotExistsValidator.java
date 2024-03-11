package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/** Common implementation of checking for create if not exists semantics. */
public class CreateIfNotExistsValidator extends AspectPayloadValidator {

  public CreateIfNotExistsValidator(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, AspectRetriever aspectRetriever) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    // This logic relies on the fact that key aspects are either explicitly created (in the batch)
    // or the key aspect is auto generated as part of the default aspects and included
    // within a batch.
    // Meaning the presence of the key aspect indicates that the entity doesn't exist and CREATE
    // should be allowed
    Map<Urn, ChangeMCP> entityKeyMap =
        changeMCPs.stream()
            .filter(item -> item.getEntitySpec().getKeyAspectName().equals(item.getAspectName()))
            .collect(Collectors.toMap(ReadItem::getUrn, Function.identity()));

    for (ChangeMCP item : changeMCPs) {
      // if the key aspect is missing in the batch, the entity exists and CREATE should be denied
      if (!entityKeyMap.containsKey(item.getUrn())) {
        exceptions.addException(
            item, "Cannot perform CREATE if not exists since the entity key already exists.");
      }
    }
    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems, @Nonnull AspectRetriever aspectRetriever) {
    return Stream.empty();
  }
}
