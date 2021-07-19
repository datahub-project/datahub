package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.datahub.upgrade.UpgradeContext;
import javax.annotation.Nonnull;


/**
 * Base interface for BackupReader used for creating the BackupIterator to retrieve EbeanAspectV2 object to be
 * ingested back into GMS
 */
public interface BackupReader {
  String getName();

  @Nonnull
  EbeanAspectBackupIterator getBackupIterator(UpgradeContext context);
}
