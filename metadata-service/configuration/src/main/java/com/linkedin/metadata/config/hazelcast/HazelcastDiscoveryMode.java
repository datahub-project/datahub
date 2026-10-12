package com.linkedin.metadata.config.hazelcast;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Decides whether Hazelcast should join a Kubernetes cluster or start as one member.
 *
 * <p>Quickstart, CI, and single-node installs leave the discovery name at {@value
 * #DEFAULT_SERVICE_DNS} and do not run in Kubernetes. That name does not resolve there, and
 * Hazelcast's own service-dns lookup then waits out the DNS timeout. A name that only resolves to
 * loopback is a single node. A failed lookup is retried. Inside Kubernetes a lookup that still
 * fails keeps Kubernetes join, so one DNS miss does not leave a pod outside the cluster. Hazelcast
 * retries its own lookup. A custom name that fails DNS outside Kubernetes also keeps Kubernetes
 * join, so a broken cluster name stays visible.
 */
public final class HazelcastDiscoveryMode {

  public static final String DEFAULT_SERVICE_DNS = "hazelcast-service";

  static final int DNS_ATTEMPTS = 3;
  static final long DNS_RETRY_PAUSE_MS = 200L;

  public enum Join {
    SINGLE_NODE,
    KUBERNETES
  }

  @FunctionalInterface
  public interface Resolver {
    InetAddress[] resolve(String name) throws UnknownHostException;
  }

  @FunctionalInterface
  interface Pause {
    void betweenAttempts() throws InterruptedException;
  }

  private HazelcastDiscoveryMode() {}

  public static Join decide(String serviceName) {
    return decide(
        serviceName,
        InetAddress::getAllByName,
        System.getenv("KUBERNETES_SERVICE_HOST"),
        () -> Thread.sleep(DNS_RETRY_PAUSE_MS));
  }

  static Join decide(String serviceName, Resolver resolver) {
    return decide(serviceName, resolver, null, () -> {});
  }

  static Join decide(
      String serviceName, Resolver resolver, String kubernetesServiceHost, Pause pause) {
    String name = serviceName == null ? "" : serviceName.trim();
    if (name.isEmpty() || isLoopbackName(name)) {
      return Join.SINGLE_NODE;
    }
    for (int attempt = 1; attempt <= DNS_ATTEMPTS; attempt++) {
      try {
        InetAddress[] addresses = resolver.resolve(name);
        if (addresses == null || addresses.length == 0 || allLoopback(addresses)) {
          return Join.SINGLE_NODE;
        }
        return Join.KUBERNETES;
      } catch (UnknownHostException e) {
        if (attempt == DNS_ATTEMPTS) {
          break;
        }
        try {
          pause.betweenAttempts();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    if (inKubernetes(kubernetesServiceHost)) {
      return Join.KUBERNETES;
    }
    if (DEFAULT_SERVICE_DNS.equals(name)) {
      return Join.SINGLE_NODE;
    }
    return Join.KUBERNETES;
  }

  private static boolean inKubernetes(String kubernetesServiceHost) {
    return kubernetesServiceHost != null && !kubernetesServiceHost.trim().isEmpty();
  }

  private static boolean isLoopbackName(String name) {
    return "localhost".equalsIgnoreCase(name)
        || "localhost.localdomain".equalsIgnoreCase(name)
        || "127.0.0.1".equals(name)
        || "::1".equals(name)
        || "0:0:0:0:0:0:0:1".equals(name);
  }

  private static boolean allLoopback(InetAddress[] addresses) {
    return Arrays.stream(addresses).allMatch(InetAddress::isLoopbackAddress);
  }
}
