package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.linkedin.datahub.upgrade.UpgradeContext;
import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;


@Slf4j
public class LocalParquetReader implements BackupReader {
  @Override
  public String getName() {
    return "LOCAL_PARQUET";
  }

  @Override
  public BackupIterator getBackupIterator(UpgradeContext context) {
    Optional<String> path = context.parsedArgs().get("BACKUP_FILE_PATH");
    if (!path.isPresent()) {
      context.report().addLine("BACKUP_FILE_PATH must be set to run RestoreBackup through local parquet file");
      return null;
    }

    try {
      ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(path.get())).build();
      return new ParquetIterator(reader);
    } catch (IOException e) {
      context.report().addLine("Failed to build ParquetReader");
      return null;
    }
  }
}
