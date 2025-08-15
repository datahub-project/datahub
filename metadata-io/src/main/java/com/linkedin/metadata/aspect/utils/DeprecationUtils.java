package com.linkedin.metadata.aspect.utils;

import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DeprecationUtils {

  private DeprecationUtils() {}

  @Nullable
  public static Deprecation getDeprecation(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService entityService,
      @Nonnull String urn,
      Urn actor,
      @Nullable String note,
      boolean deprecated,
      @Nullable Long decommissionTime,
      @Nullable String replacementUrn) {
    Deprecation deprecation =
        (Deprecation)
            EntityUtils.getAspectFromEntity(
                opContext,
                urn,
                Constants.DEPRECATION_ASPECT_NAME,
                entityService,
                new Deprecation());
    if (deprecation == null) {
      return null;
    }
    deprecation.setActor(actor);
    deprecation.setDeprecated(deprecated);
    try {
      deprecation.setReplacement(
          replacementUrn != null ? Urn.createFromString(replacementUrn) : null,
          SetMode.REMOVE_IF_NULL);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    deprecation.setDecommissionTime(decommissionTime, SetMode.REMOVE_IF_NULL);
    deprecation.setNote(Objects.requireNonNullElse(note, ""));
    return deprecation;
  }
}
