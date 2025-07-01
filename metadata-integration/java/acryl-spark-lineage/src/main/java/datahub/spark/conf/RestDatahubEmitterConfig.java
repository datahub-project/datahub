package datahub.spark.conf;

import datahub.client.rest.RestEmitterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@ToString
@Getter
public class RestDatahubEmitterConfig implements DatahubEmitterConfig {
  final String type = "rest";
  datahub.client.rest.RestEmitterConfig restEmitterConfig;

  public RestDatahubEmitterConfig(RestEmitterConfig restEmitterConfig) {
    this.restEmitterConfig = restEmitterConfig;
  }
}
