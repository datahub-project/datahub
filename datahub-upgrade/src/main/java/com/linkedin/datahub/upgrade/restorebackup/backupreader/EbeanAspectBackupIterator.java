/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Base interface for iterators that retrieves EbeanAspectV2 objects This allows us to restore from
 * backups of various format
 */
@Slf4j
@RequiredArgsConstructor
public class EbeanAspectBackupIterator<T extends ReaderWrapper> implements Closeable {

  private final Collection<T> _readers;
  private final Iterator<T> it;

  public EbeanAspectBackupIterator(final Collection<T> readers) {
    this._readers = readers;
    it = _readers.iterator();
  }

  public T getNextReader() {
    while (it.hasNext()) {
      final T element = it.next();
      log.warn("Iterating over reader {}", element.getFileName());
      return element;
    }
    return null;
  }

  @Override
  public void close() {
    _readers.forEach(
        reader -> {
          try {
            reader.close();
          } catch (IOException e) {
            log.error("Error while closing parquet reader", e);
          }
        });
  }
}
