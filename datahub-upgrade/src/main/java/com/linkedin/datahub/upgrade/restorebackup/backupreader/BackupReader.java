package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.datahub.upgrade.UpgradeContext;


public interface BackupReader {
  String getName();

  BackupIterator getBackupIterator(UpgradeContext context);
}
