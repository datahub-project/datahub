package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.SYSTEM_POLICY_ONE;
import static com.linkedin.metadata.Constants.SYSTEM_POLICY_ZERO;


/**
 * Validator to ensure that system policies (i. e: Those can come natively with DataHub) are not
 * deleted. *
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class SystemPolicyValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  private Set<ChangeType> MODIFY_CHANGE_TYPES = Set.of(ChangeType.PATCH, ChangeType.UPDATE, ChangeType.UPSERT);

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    mcpItems.forEach(
        i -> {
          final Urn entityUrn = i.getUrn();
          if (isSystemPolicy(entityUrn) && ChangeType.DELETE.equals(i.getChangeType())) {
            exceptions.addException(
                AspectValidationException.forItem(i, "System policies can not be deleted."));
          } else if (MODIFY_CHANGE_TYPES.contains(i.getChangeType()) &&
              // We don't know actor or we know it isn't the system actor, system actor is allowed to modify to allow
              // reingestion of base policies if they have for example been directly modified in the database
              (i.getAuditStamp() == null || !SYSTEM_ACTOR.equals(i.getAuditStamp().getActor().toString()))) {
            final Aspect aspect =
                retrieverContext
                    .getAspectRetriever()
                    .getLatestAspectObject(entityUrn, Constants.DATAHUB_POLICY_INFO_ASPECT_NAME);
            if (aspect != null) {
              DataHubPolicyInfo dataHubPolicyInfo = new DataHubPolicyInfo(aspect.data());
              if (!dataHubPolicyInfo.isEditable()) {
                exceptions.addException(
                    AspectValidationException.forItem(i, "Attempting to edit not editable policy."));
              }
            }
          }
        });

    return exceptions.streamAllExceptions();
  }

  private boolean isSystemPolicy(Urn entityUrn) {
    return SYSTEM_POLICY_ZERO.equals(entityUrn) || SYSTEM_POLICY_ONE.equals(entityUrn);
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
