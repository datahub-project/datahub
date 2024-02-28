package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DEFAULT_OWNERSHIP_TYPE_URN;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Hook to populate the ownerType map within the ownership aspect */
public class OwnerTypeMap extends MutationHook {
  public OwnerTypeMap(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull AspectRetriever aspectRetriever) {

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

        Set<Urn> updatedTypes = newTypeOwner.keySet();

        Map<String, UrnArray> typeOwners =
            Stream.concat(removedTypes.stream(), updatedTypes.stream())
                .map(
                    typeUrn -> {
                      final String typeFieldName = encodeFieldName(typeUrn.toString());
                      if (removedTypes.contains(typeUrn)) {
                        // removed
                        return Pair.of(typeFieldName, new UrnArray());
                      }
                      // updated
                      return Pair.of(
                          typeFieldName,
                          new UrnArray(
                              newTypeOwner.getOrDefault(typeUrn, Collections.emptySet()).stream()
                                  .map(Owner::getOwner)
                                  .collect(Collectors.toSet())));
                    })
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

        if (!typeOwners.isEmpty()) {
          item.getAspect(Ownership.class).setOwnerTypes(new UrnArrayMap(typeOwners));
          results.add(Pair.of(item, true));
          continue;
        }
      }

      // no op
      results.add(Pair.of(item, false));
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
                Collectors.groupingBy(
                    owner ->
                        owner.getTypeUrn() != null
                            ? owner.getTypeUrn()
                            : DEFAULT_OWNERSHIP_TYPE_URN,
                    Collectors.toSet()));
      }
    }
    return Collections.emptyMap();
  }

  public static String encodeFieldName(String value) {
    return value.replaceAll("[.]", "%2E");
  }

  public static String decodeFieldName(String value) {
    return value.replaceAll("%2E", ".");
  }
}
