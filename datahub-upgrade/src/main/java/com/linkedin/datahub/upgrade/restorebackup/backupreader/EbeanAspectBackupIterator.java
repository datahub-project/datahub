package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import java.io.Closeable;


/**
 * Base interface for iterators that retrieves EbeanAspectV2 objects
 * This allows us to restore from backups of various format
 */
public interface EbeanAspectBackupIterator extends Closeable {
  // Get the next row in backup. Return null if finished.
  EbeanAspectV2 next();
}
