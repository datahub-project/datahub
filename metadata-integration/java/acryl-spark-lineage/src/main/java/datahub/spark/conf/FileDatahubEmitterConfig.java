package datahub.spark.conf;

import datahub.client.file.FileEmitterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@ToString
@Getter
public class FileDatahubEmitterConfig implements DatahubEmitterConfig {
  final String type = "file";
  FileEmitterConfig fileEmitterConfig;

  public FileDatahubEmitterConfig(FileEmitterConfig fileEmitterConfig) {
    this.fileEmitterConfig = fileEmitterConfig;
  }
}
