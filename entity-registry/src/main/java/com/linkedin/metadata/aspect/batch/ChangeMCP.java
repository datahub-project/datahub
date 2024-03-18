package com.linkedin.metadata.aspect.batch;

import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.SystemAspect;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A proposal to write data to the primary datastore which includes system metadata and other
 * related data stored along with the aspect
 */
public interface ChangeMCP extends MCPItem {
  @Nonnull
  SystemAspect getSystemAspect(@Nullable Long nextAspectVersion);

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
}
