/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
