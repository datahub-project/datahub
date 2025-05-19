package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;

import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.util.Pair;
import java.util.*;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
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

  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return changeMCPS.stream()
        .map(
            item ->
                aspectFilter(item)
                    ? Pair.of(item, processOwnershipAspect(item))
                    : Pair.of(item, false));
  }

  private static boolean aspectFilter(ReadItem item) {
    return item.getAspectName().equals(OWNERSHIP_ASPECT_NAME);
  }

  public static Map<String, List<Urn>> toMap(UrnArrayMap map) {
    Map<String, List<Urn>> result = new HashMap<>();
    map.forEach(
        ((s, urns) -> {
          result.put(s, new ArrayList<>(urns));
        }));
    return result;
  }

  public static UrnArrayMap toUrnArrayMap(Map<String, List<Urn>> map) {
    UrnArrayMap result = new UrnArrayMap();
    map.forEach(
        (key, urns) -> {
          result.put(key, new UrnArray(urns));
        });
    return result;
  }

  public static boolean processOwnershipAspect(ChangeMCP item) {
    boolean mutated = false;
    Ownership ownership = item.getAspect(Ownership.class);
    if (ownership == null) {
      return false;
    }
    UrnArrayMap ownerTypes = ownership.getOwnerTypes();
    Map<String, List<Urn>> ownerTypesMap;
    if (ownerTypes == null) {
      ownerTypesMap = new HashMap<>();
      mutated = true;
    } else {
      ownerTypesMap = toMap(ownerTypes);
    }
    OwnerArray owners = ownership.getOwners();
    for (Owner owner : owners) {
      String typeKey =
          Optional.ofNullable(owner.getTypeUrn())
              .map(Urn::toString)
              .orElseGet(
                  () ->
                      "urn:li:ownershipType:__system__" + owner.getType().toString().toLowerCase());

      List<Urn> ownerOfType;
      if (ownerTypesMap.containsKey(typeKey)) {
        ownerOfType = ownerTypesMap.get(typeKey);
      } else {
        ownerOfType = new ArrayList<>();
        ownerTypesMap.put(typeKey, ownerOfType);
        mutated = true;
      }
      Urn ownerUrn = owner.getOwner();
      if (!ownerOfType.contains(ownerUrn)) {
        ownerOfType.add(ownerUrn);
        mutated = true;
      }
    }
    if (mutated) {
      ownership.setOwnerTypes((toUrnArrayMap(ownerTypesMap)));
    }
    return mutated;
  }
}
