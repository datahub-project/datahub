package com.linkedin.metadata.aspect.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** An item that represents a change that has been written to primary storage. */
public interface MCLItem extends BatchItem {

  @Nonnull
  MetadataChangeLog getMetadataChangeLog();

  @Override
  default Urn getUrn() {
    return getMetadataChangeLog().getEntityUrn();
  }

  @Nonnull
  @Override
  default String getAspectName() {
    if (getMetadataChangeLog().getAspectName() != null) {
      return getMetadataChangeLog().getAspectName();
    } else {
      return getRecordTemplate().schema().getName();
    }
  }

  @Override
  default SystemMetadata getSystemMetadata() {
    return getMetadataChangeLog().getSystemMetadata();
  }

  default SystemMetadata getPreviousSystemMetadata() {
    return getMetadataChangeLog().getPreviousSystemMetadata();
  }

  @Nullable
  RecordTemplate getPreviousRecordTemplate();

  default <T> T getPreviousAspect(Class<T> clazz) {
    if (getPreviousRecordTemplate() != null) {
      try {
        return clazz.getConstructor(DataMap.class).newInstance(getPreviousRecordTemplate().data());
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

  @Override
  @Nonnull
  default ChangeType getChangeType() {
    return getMetadataChangeLog().getChangeType();
  }

  @Override
  default AuditStamp getAuditStamp() {
    return getMetadataChangeLog().getCreated();
  }

  /**
   * Change detection for previous and new record template
   *
   * @return no change detection
   */
  default boolean isNoOp() {
    return getPreviousRecordTemplate() == getRecordTemplate();
  }
}
