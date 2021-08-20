package com.linkedin.gms.factory.common;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Slf4j
@Configuration
public class DgraphClientFactory {
  @Value("${DGRAPH_HOST:localhost}")
  private String[] hosts;

  @Value("${DGRAPH_GRPC_PORT:9080}")
  private int port;

  @Value("${DGRAPH_SECURITY:plain}")
  private String security;

  @Bean(name = "dgraphClient")
  protected DgraphClient createInstance() {
    DgraphGrpc.DgraphStub[] stubs = Arrays.stream(hosts)
            .map(this::getChannelForHost)
            .map(DgraphGrpc::newStub)
            .toArray(DgraphGrpc.DgraphStub[]::new);

    return new DgraphClient(stubs);
  }

  private ManagedChannel getChannelForHost(String host) {
    log.info("Connecting to host " + host);
    if (host.contains(":")) {
      return getChannelForBuilder(ManagedChannelBuilder.forTarget(host));
    } else {
      return getChannelForBuilder(ManagedChannelBuilder.forAddress(host, port));
    }
  }

  private ManagedChannel getChannelForBuilder(ManagedChannelBuilder<?> builder) {
    if (security.equalsIgnoreCase("plain")) {
      builder.usePlaintext();
    } else if (security.equalsIgnoreCase("tls")) {
      builder.useTransportSecurity();
    } else {
      throw new IllegalArgumentException("Unsupported channel security mode");
    }

    return builder.build();
  }
}
