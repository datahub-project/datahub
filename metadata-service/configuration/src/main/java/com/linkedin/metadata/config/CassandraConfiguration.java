package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CassandraConfiguration {
  private String datasourceUsername;
  private String datasourcePassword;
  private String hosts;
  private String port;
  private String datacenter;
  private String keyspace;
  private boolean useSsl;
  private ReadPoolConfiguration readPool;
}
