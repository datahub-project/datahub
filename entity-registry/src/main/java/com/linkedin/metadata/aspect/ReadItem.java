package com.linkedin.metadata.aspect;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.SystemMetadata;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ReadItem {
  /**
   * The urn associated with the aspect
   *
   * @return
   */
  @Nonnull
  Urn getUrn();

  /**
   * Aspect's name
   *
   * @return the name
   */
  @Nonnull
  default String getAspectName() {
    if (getAspectSpec() == null) {
      return GenericAspect.dataSchema().getName();
    }
    return getAspectSpec().getName();
  }

  @Nullable
  RecordTemplate getRecordTemplate();

  @Nullable
  default <T> T getAspect(Class<T> clazz) {
    return getAspect(clazz, getRecordTemplate());
  }

  static <T> T getAspect(Class<T> clazz, @Nullable RecordTemplate recordTemplate) {
    if (recordTemplate != null) {
      try {
        return clazz.getConstructor(DataMap.class).newInstance(recordTemplate.data());
      } catch (InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    } else {
      return null;
    }
  }

  /**
   * System information
   *
   * @return the system metadata
   */
  @Nullable
  SystemMetadata getSystemMetadata();

  /**
   * The entity's schema
   *
   * @return entity specification
   */
  @Nonnull
  EntitySpec getEntitySpec();

  /**
   * The aspect's schema
   *
   * @return aspect's specification
   */
  @Nullable
  AspectSpec getAspectSpec();
}
