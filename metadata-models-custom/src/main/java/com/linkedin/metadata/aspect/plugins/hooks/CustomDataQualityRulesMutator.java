package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import com.mycompany.dq.DataQualityRule;
import com.mycompany.dq.DataQualityRules;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class CustomDataQualityRulesMutator extends MutationHook {

  public CustomDataQualityRulesMutator(AspectPluginConfig config) {
    super(config);
  }

  @Override
  protected void mutate(
      @Nonnull ChangeType changeType,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate oldAspectValue,
      @Nullable RecordTemplate newAspectValue,
      @Nullable SystemMetadata oldSystemMetadata,
      @Nullable SystemMetadata newSystemMetadata,
      @Nonnull AuditStamp auditStamp,
      @Nonnull AspectRetriever aspectRetriever) {

    if (newAspectValue != null) {
      DataQualityRules newDataQualityRules = new DataQualityRules(newAspectValue.data());

      for (DataQualityRule rule : newDataQualityRules.getRules()) {
        // Ensure uniform lowercase
        if (!rule.getType().toLowerCase().equals(rule.getType())) {
          rule.setType(rule.getType().toLowerCase());
        }
      }
    }
  }
}
