package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.datahub.upgrade.UpgradeContext;
import javax.annotation.Nonnull;

/**
 * Base interface for BackupReader used for creating the BackupIterator to retrieve EbeanAspectV2
 * object to be ingested back into GMS. Must have a constructor that takes a List of Optional
 * Strings
 */
public interface BackupReader<T extends ReaderWrapper> {
  String getName();

  @Nonnull
  EbeanAspectBackupIterator<T> getBackupIterator(UpgradeContext context);
}
