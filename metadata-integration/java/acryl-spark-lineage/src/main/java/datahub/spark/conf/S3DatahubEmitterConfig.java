package datahub.spark.conf;

import datahub.client.s3.S3EmitterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@ToString
@Getter
public class S3DatahubEmitterConfig implements DatahubEmitterConfig {
  final String type = "s3";
  S3EmitterConfig s3EmitterConfig;

  public S3DatahubEmitterConfig(S3EmitterConfig s3EmitterConfig) {
    this.s3EmitterConfig = s3EmitterConfig;
  }
}
