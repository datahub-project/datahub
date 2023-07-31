package com.linkedin.metadata.service.util;

import com.linkedin.data.template.StringMap;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class MetadataTestServiceUtils {

  private MetadataTestServiceUtils() {

  }

  public static void applyAppSource(@Nonnull List<MetadataChangeProposal> changes, @Nonnull String appSource) {
    changes.forEach(change -> {
      SystemMetadata systemMetadata = Optional.ofNullable(change.getSystemMetadata()).orElse(new SystemMetadata());
      StringMap properties = Optional.ofNullable(systemMetadata.getProperties()).orElse(new StringMap());
      properties.put(APP_SOURCE, appSource);
      systemMetadata.setProperties(properties);
      change.setSystemMetadata(systemMetadata);
    });
  }
}
