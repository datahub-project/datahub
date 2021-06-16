package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import java.io.Closeable;


public interface BackupIterator extends Closeable {
  // Get the next row in backup. Return null if finished.
  EbeanAspectV2 next();
}
