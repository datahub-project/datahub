package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;


/**
 * Iterator to retrieve EbeanAspectV2 objects from the ParquetReader
 * Converts the avro GenericRecord object into EbeanAspectV2
 */
@RequiredArgsConstructor
public class ParquetEbeanAspectBackupIterator implements EbeanAspectBackupIterator {
  private final ParquetReader<GenericRecord> _parquetReader;

  @Override
  public EbeanAspectV2 next() {
    try {
      GenericRecord record = _parquetReader.read();
      if (record == null) {
        return null;
      }
      return convertRecord(record);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    _parquetReader.close();
  }

  private EbeanAspectV2 convertRecord(GenericRecord record) {
    EbeanAspectV2.PrimaryKey key =
        new EbeanAspectV2.PrimaryKey(record.get("urn").toString(), record.get("aspect").toString(),
            (Long) record.get("version"));
    return new EbeanAspectV2(key, record.get("metadata").toString(),
        Timestamp.from(Instant.ofEpochMilli((Long) record.get("createdon") / 1000)), record.get("createdby").toString(),
        Optional.ofNullable(record.get("createdfor")).map(Object::toString).orElse(null));
  }
}
