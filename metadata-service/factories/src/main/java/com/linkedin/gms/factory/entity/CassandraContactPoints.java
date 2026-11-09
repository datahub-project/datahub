package com.linkedin.gms.factory.entity;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Parses a comma-separated {@code cassandra.hosts} value into driver contact points. */
final class CassandraContactPoints {

  private CassandraContactPoints() {}

  /**
   * Trims each entry and skips blank ones, so {@code "host1, host2"} or a trailing comma do not
   * produce a contact point with a leading space or an empty host (which resolves to localhost).
   *
   * @throws IllegalArgumentException if no host is left, since the driver would otherwise fall back
   *     to 127.0.0.1:9042 and ignore the configured port
   */
  @Nonnull
  static List<InetSocketAddress> parse(@Nonnull String hosts, int port) {
    List<InetSocketAddress> addresses =
        Arrays.stream(hosts.split(","))
            .map(String::trim)
            .filter(host -> !host.isEmpty())
            .map(host -> new InetSocketAddress(host, port))
            .collect(Collectors.toList());
    if (addresses.isEmpty()) {
      throw new IllegalArgumentException("No Cassandra contact points in hosts: '" + hosts + "'");
    }
    return addresses;
  }
}
