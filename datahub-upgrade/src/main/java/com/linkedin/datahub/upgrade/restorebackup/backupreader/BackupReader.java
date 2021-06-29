package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.datahub.upgrade.UpgradeContext;
import java.util.Optional;


/**
 * Base interface for BackupReader used for creating the BackupIterator to retrieve EbeanAspectV2 object to be
 * ingested back into GMS
 */
public interface BackupReader {
  String getName();

  Optional<EbeanAspectBackupIterator> getBackupIterator(UpgradeContext context);
}
