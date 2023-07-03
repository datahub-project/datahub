package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import java.io.Closeable;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;


/**
 * Abstract class that reads entries from a given source and transforms then into {@link EbeanAspectV2} instances.
 * @param <T> The object type to read from a reader source.
 */
@Slf4j
public abstract class ReaderWrapper<T> implements Closeable {

  private long totalTimeSpentInRead = 0L;
  private long lastTimeLogged = 0L;
  private int recordsSkipped = 0;
  private int recordsFailed = 0;
  private int recordsProcessed = 0;
  private long totalTimeSpentInConvert = 0L;
  private final String _fileName;

  ReaderWrapper(String fileName) {
    this._fileName = fileName;
  }

  public EbeanAspectV2 next() {
    try {
      long readStart = System.nanoTime();
      T record = read();
      long readEnd = System.nanoTime();
      totalTimeSpentInRead += readEnd - readStart;

      while ((record != null) && !isLatestVersion(record)) {
        recordsSkipped += 1;
        readStart = System.nanoTime();
        record = read();
        readEnd = System.nanoTime();
        totalTimeSpentInRead += readEnd - readStart;
      }
      if ((readEnd - lastTimeLogged) > 1000L * 1000 * 1000 * 5) {
        // print every 5 seconds
        printStat("Running: ");
        lastTimeLogged = readEnd;
      }
      if (record == null) {
        printStat("Closing: ");
        close();
        return null;
      }
      long convertStart = System.nanoTime();
      final EbeanAspectV2 ebeanAspectV2 = convertRecord(record);
      long convertEnd = System.nanoTime();
      this.totalTimeSpentInConvert += convertEnd - convertStart;
      this.recordsProcessed++;
      return ebeanAspectV2;
    } catch (Exception e) {
      log.error("Error while reading backed up aspect", e);
      this.recordsFailed++;
      return null;
    }
  }

  abstract T read() throws IOException;

  abstract boolean isLatestVersion(T record);

  abstract EbeanAspectV2 convertRecord(T record);

  private void printStat(String prefix) {
    log.info("{} Reader {}. Stats: records processed: {}, Total millis spent in reading: {}, records skipped: {},"
            + " records failed: {}, Total millis in convert: {}", prefix, _fileName,
        recordsProcessed, totalTimeSpentInRead / 1000 / 1000, recordsSkipped, recordsFailed,
        totalTimeSpentInConvert / 1000 / 1000);
  }

  public String getFileName() {
    return _fileName;
  }
}
