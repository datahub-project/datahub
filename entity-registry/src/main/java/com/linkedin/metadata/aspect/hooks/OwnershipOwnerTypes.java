package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DEFAULT_OWNERSHIP_TYPE_URN;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.util.Pair;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class OwnershipOwnerTypes extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {

    List<Pair<ChangeMCP, Boolean>> results = new LinkedList<>();

    for (ChangeMCP item : changeMCPS) {
      if (OWNERSHIP_ASPECT_NAME.equals(item.getAspectName()) && item.getRecordTemplate() != null) {
        final Map<Urn, Set<Owner>> oldTypeOwner =
            groupByOwnerType(item.getPreviousRecordTemplate());
        final Map<Urn, Set<Owner>> newTypeOwner = groupByOwnerType(item.getRecordTemplate());

        Set<Urn> removedTypes =
            oldTypeOwner.keySet().stream()
                .filter(typeUrn -> !newTypeOwner.containsKey(typeUrn))
                .collect(Collectors.toSet());

        // Only update if there are actual changes
        if (!removedTypes.isEmpty() || !oldTypeOwner.equals(newTypeOwner)) {
          // Build complete typeOwners map with ALL types
          Map<String, UrnArray> typeOwners =
              newTypeOwner.entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          e -> encodeFieldName(e.getKey().toString()),
                          e ->
                              new UrnArray(
                                  e.getValue().stream()
                                      .map(Owner::getOwner)
                                      .collect(Collectors.toSet()))));

          item.getAspect(Ownership.class).setOwnerTypes(new UrnArrayMap(typeOwners));
          results.add(Pair.of(item, true));
        } else {
          // No changes detected
          results.add(Pair.of(item, false));
        }
      } else {
        // Not an ownership aspect
        results.add(Pair.of(item, false));
      }
    }

    return results.stream();
  }

  private static Map<Urn, Set<Owner>> groupByOwnerType(
      @Nullable RecordTemplate ownershipRecordTemplate) {
    if (ownershipRecordTemplate != null) {
      Ownership ownership = new Ownership(ownershipRecordTemplate.data());
      if (!ownership.getOwners().isEmpty()) {
        return ownership.getOwners().stream()
            .collect(
                Collectors.groupingBy(OwnershipOwnerTypes::resolveToTypeUrn, Collectors.toSet()));
      }
    }
    return Collections.emptyMap();
  }

  private static Urn resolveToTypeUrn(Owner owner) {
    if (owner.getTypeUrn() != null) {
      return owner.getTypeUrn();
    } else if (owner.hasType()) {
      return UrnUtils.getUrn(
          String.format(
              "urn:li:ownershipType:__system__%s", owner.getType().toString().toLowerCase()));
    } else {
      return DEFAULT_OWNERSHIP_TYPE_URN;
    }
  }

  public static String encodeFieldName(String value) {
    return value.replaceAll("[.]", "%2E");
  }

  public static String decodeFieldName(String value) {
    return value.replaceAll("%2E", ".");
  }
}
