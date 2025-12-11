/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Retains a map of what arguments are passed in to a backup reader */
public final class BackupReaderArgs {
  private BackupReaderArgs() {}

  private static final Map<Class<? extends BackupReader>, List<String>> ARGS_MAP;

  static {
    ARGS_MAP = new HashMap<>();
    ARGS_MAP.put(LocalParquetReader.class, LocalParquetReader.argNames());
  }

  public static List<String> getArgNames(Class<? extends BackupReader> clazz) {
    return ARGS_MAP.get(clazz);
  }
}
