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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class OwnershipOwnerTypes extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    List<Pair<ChangeMCP, Boolean>> results = new LinkedList<>();
    for (ChangeMCP item : changeMCPS) {
      if (aspectFilter(item)) {
        results.add(Pair.of(item, processOwnershipAspect(item)));
      } else {
        results.add(Pair.of(item, false));
      }
    }
    return results.stream();
  }

  private static boolean aspectFilter(ReadItem item) {
    return item.getAspectName().equals(OWNERSHIP_ASPECT_NAME);
  }

  public static boolean processOwnershipAspect(ChangeMCP item) {
    boolean mutated = false;
    Ownership ownership = item.getAspect(Ownership.class);
    if (ownership == null) {
      return false;
    }
    UrnArrayMap ownerTypes = ownership.getOwnerTypes();
    if (ownerTypes == null) {
      ownerTypes = ownership.getOwnerTypes();
      ownership.setOwnerTypes(ownerTypes);
      mutated = true;
    }
    OwnerArray owners = ownership.getOwners();
    for (Owner owner : owners) {
      Urn typeUrn = owner.getTypeUrn();
      String typeUrnStr = null;
      if (typeUrn != null) {
        typeUrnStr = typeUrn.toString();
      }
      UrnArray ownerOfType;
      boolean found = false;
      if (typeUrnStr == null) {
        OwnershipType type = owner.getType();
        String typeStr = "urn:li:ownershipType:__system__" + type.toString().toLowerCase();
        if (ownerTypes.containsKey(typeStr)) {
          ownerOfType = ownerTypes.get(typeUrnStr);
        } else {
          ownerOfType = new UrnArray();
          ownerTypes.put(typeStr, ownerOfType);
          mutated = true;
        }
      } else {
        if (ownerTypes.containsKey(typeUrnStr)) {
          ownerOfType = ownerTypes.get(typeUrnStr);
        } else {
          ownerOfType = new UrnArray();
          ownerTypes.put(typeUrnStr, ownerOfType);
          mutated = true;
        }
      }
      for (Urn ownerUrn : ownerOfType) {
        if (ownerUrn.equals(owner.getOwner())) {
          found = true;
        }
      }
      if (!found) {
        ownerOfType.add(owner.getOwner());
        mutated = true;
      }
    }
    return mutated;
  }
}
