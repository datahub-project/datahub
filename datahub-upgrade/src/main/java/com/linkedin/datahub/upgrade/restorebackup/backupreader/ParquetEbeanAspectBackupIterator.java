package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;


/**
 * Iterator to retrieve EbeanAspectV2 objects from the ParquetReader
 * Converts the avro GenericRecord object into EbeanAspectV2
 */
@Slf4j
@RequiredArgsConstructor
public class ParquetEbeanAspectBackupIterator implements EbeanAspectBackupIterator {
  private final List<ParquetReader<GenericRecord>> _parquetReaders;
  private int currentReaderIndex = 0;

  @Override
  public EbeanAspectV2 next() {

    if (currentReaderIndex >= _parquetReaders.size()) {
      return null;
    }
    ParquetReader<GenericRecord> parquetReader = _parquetReaders.get(currentReaderIndex);

    try {
      GenericRecord record = parquetReader.read();
      if (record == null) {
        log.info("Record is null, moving to next reader {} {}", currentReaderIndex, _parquetReaders.size());
        parquetReader.close();
        currentReaderIndex++;
        return next();
      }
      return convertRecord(record);
    } catch (IOException e) {
      log.error("Error while reading backed up aspect", e);
      return null;
    }
  }

  @Override
  public void close() {
    _parquetReaders.forEach(reader -> {
      try {
        reader.close();
      } catch (IOException e) {
        log.error("Error while closing parquet reader", e);
      }
    });
  }

  private EbeanAspectV2 convertRecord(GenericRecord record) {
    return new EbeanAspectV2(record.get("urn").toString(), record.get("aspect").toString(),
        (Long) record.get("version"), record.get("metadata").toString(),
        Timestamp.from(Instant.ofEpochMilli((Long) record.get("createdon") / 1000)), record.get("createdby").toString(),
        Optional.ofNullable(record.get("createdfor")).map(Object::toString).orElse(null),
        Optional.ofNullable(record.get("systemmetadata")).map(Object::toString).orElse(null));
  }
}
