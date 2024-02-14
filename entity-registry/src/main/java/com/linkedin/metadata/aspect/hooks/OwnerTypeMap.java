package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DEFAULT_OWNERSHIP_TYPE_URN;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Hook to populate the ownerType map within the ownership aspect */
public class OwnerTypeMap extends MutationHook {
  public OwnerTypeMap(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
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
    if (OWNERSHIP_ASPECT_NAME.equals(aspectSpec.getName()) && newAspectValue != null) {
      Ownership ownership = new Ownership(newAspectValue.data());
      if (!ownership.getOwners().isEmpty()) {

        Map<Urn, Set<Owner>> ownerTypes =
            ownership.getOwners().stream()
                .collect(Collectors.groupingBy(Owner::getOwner, Collectors.toSet()));

        ownership.setOwnerTypes(
            new UrnArrayMap(
                ownerTypes.entrySet().stream()
                    .map(
                        entry ->
                            Pair.of(
                                encodeFieldName(entry.getKey().toString()),
                                new UrnArray(
                                    entry.getValue().stream()
                                        .map(
                                            owner ->
                                                owner.getTypeUrn() != null
                                                    ? owner.getTypeUrn()
                                                    : DEFAULT_OWNERSHIP_TYPE_URN)
                                        .collect(Collectors.toSet()))))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue))));
      }
    }
  }

  public static String encodeFieldName(String value) {
    return value.replaceAll("[.]", "%2E");
  }

  public static String decodeFieldName(String value) {
    return value.replaceAll("%2E", ".");
  }
}
