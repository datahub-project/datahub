package com.linkedin.metadata.aspect.batch;

import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.SystemAspect;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * A proposal to write data to the primary datastore which includes system metadata and other
 * related data stored along with the aspect
 */
public interface ChangeMCP extends MCPItem {
  @Nullable
  SystemAspect getPreviousSystemAspect();

  void setPreviousSystemAspect(@Nullable SystemAspect previousSystemAspect);

  long getNextAspectVersion();

  void setNextAspectVersion(long nextAspectVersion);

  @Nullable
  default RecordTemplate getPreviousRecordTemplate() {
    if (getPreviousSystemAspect() != null) {
      return getPreviousSystemAspect().getRecordTemplate();
    }
    return null;
  }

  default <T> T getPreviousAspect(Class<T> clazz) {
    if (getPreviousSystemAspect() != null) {
      try {
        return clazz
            .getConstructor(DataMap.class)
            .newInstance(getPreviousSystemAspect().getRecordTemplate().data());
      } catch (InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  default String toAbbreviatedString() {
    return "ChangeMCP{"
        + "changeType="
        + getChangeType()
        + ", urn="
        + getUrn()
        + ", aspectName='"
        + getAspectName()
        + '\''
        + ", recordTemplate="
        + Optional.ofNullable(getRecordTemplate())
            .map(template -> StringUtils.abbreviate(template.toString(), 256))
            .orElse("")
        + ", systemMetadata="
        + Optional.ofNullable(getSystemMetadata())
            .map(systemMetadata -> StringUtils.abbreviate(systemMetadata.toString(), 128))
            .orElse("")
        + '}';
  }
}
