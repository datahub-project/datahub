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
   * Trims each entry and skips blank ones, so {@code "host1, host2"} does not produce a contact
   * point with a leading space and {@code "host1,,host2"} does not add an empty host, which would
   * resolve to localhost.
   *
   * @param setting the property the hosts came from, named in the error
   * @throws IllegalArgumentException if no host is left, rather than connecting to localhost
   */
  @Nonnull
  static List<InetSocketAddress> parse(@Nonnull String hosts, int port, @Nonnull String setting) {
    List<InetSocketAddress> addresses =
        Arrays.stream(hosts.split(","))
            .map(String::strip)
            .filter(host -> !host.isEmpty())
            .map(host -> new InetSocketAddress(host, port))
            .collect(Collectors.toList());
    if (addresses.isEmpty()) {
      throw new IllegalArgumentException(
          setting + " has no hosts; set it to a comma-separated list of Cassandra hosts");
    }
    return addresses;
  }
}
