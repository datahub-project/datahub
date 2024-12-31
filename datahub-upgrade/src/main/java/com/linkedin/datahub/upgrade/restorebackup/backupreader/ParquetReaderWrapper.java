package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;

@Slf4j
public class ParquetReaderWrapper extends ReaderWrapper<GenericRecord> {

  private static final long NANOS_PER_MILLISECOND = 1000000;
  private static final long MILLIS_IN_DAY = 86400000;
  private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;

  private final ParquetReader<GenericRecord> _parquetReader;

  public ParquetReaderWrapper(ParquetReader<GenericRecord> parquetReader, String fileName) {
    super(fileName);
    _parquetReader = parquetReader;
  }

  @Override
  boolean isLatestVersion(GenericRecord record) {
    return (Long) record.get("version") == 0L;
  }

  @Override
  GenericRecord read() throws IOException {
    return _parquetReader.read();
  }

  EbeanAspectV2 convertRecord(GenericRecord record) {

    long ts;
    if (record.get("createdon") instanceof GenericFixed) {
      ts = convertFixed96IntToTs((GenericFixed) record.get("createdon"));
    } else {
      ts = (Long) record.get("createdon");
    }

    return new EbeanAspectV2(
        record.get("urn").toString(),
        record.get("aspect").toString(),
        (Long) record.get("version"),
        record.get("metadata").toString(),
        Timestamp.from(Instant.ofEpochMilli(ts / 1000)),
        record.get("createdby").toString(),
        Optional.ofNullable(record.get("createdfor")).map(Object::toString).orElse(null),
        Optional.ofNullable(record.get("systemmetadata")).map(Object::toString).orElse(null));
  }

  private long convertFixed96IntToTs(GenericFixed createdon) {
    // From https://github.com/apache/parquet-format/pull/49/filesParquetTimestampUtils.java
    // and ParquetTimestampUtils.java from
    // https://github.com/kube-reporting/presto/blob/master/presto-parquet/
    // src/main/java/io/prestosql/parquet/ParquetTimestampUtils.java

    byte[] bytes = createdon.bytes(); // little endian encoding - need to invert byte order
    long timeOfDayNanos =
        Longs.fromBytes(
            bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
    int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);
    return ((julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY)
        + (timeOfDayNanos / NANOS_PER_MILLISECOND);
  }

  @Override
  public void close() throws IOException {
    _parquetReader.close();
  }
}
