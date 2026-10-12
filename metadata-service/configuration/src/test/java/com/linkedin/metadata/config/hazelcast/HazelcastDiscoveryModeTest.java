package com.linkedin.metadata.config.hazelcast;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.hazelcast.HazelcastDiscoveryMode.Join;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.testng.annotations.Test;

public class HazelcastDiscoveryModeTest {

  @Test
  public void defaultNameDnsFailureIsSingleNode() {
    assertEquals(
        HazelcastDiscoveryMode.decide(
            HazelcastDiscoveryMode.DEFAULT_SERVICE_DNS,
            name -> {
              throw new UnknownHostException(name);
            }),
        Join.SINGLE_NODE);
  }

  @Test
  public void loopbackNameIsSingleNode() {
    assertEquals(
        HazelcastDiscoveryMode.decide(
            "localhost",
            name -> {
              throw new UnknownHostException(name);
            }),
        Join.SINGLE_NODE);
    assertEquals(
        HazelcastDiscoveryMode.decide(
            "hazelcast-service", name -> new InetAddress[] {InetAddress.getByName("127.0.0.1")}),
        Join.SINGLE_NODE);
  }

  @Test
  public void nonLoopbackAddressKeepsKubernetesJoin() throws Exception {
    InetAddress nonLoopback = InetAddress.getByName("8.8.8.8");
    assertEquals(
        HazelcastDiscoveryMode.decide("hazelcast-service", name -> new InetAddress[] {nonLoopback}),
        Join.KUBERNETES);
  }

  @Test
  public void customNameDnsFailureKeepsKubernetesJoin() {
    assertEquals(
        HazelcastDiscoveryMode.decide(
            "hazelcast.prod.svc.cluster.local",
            name -> {
              throw new UnknownHostException(name);
            }),
        Join.KUBERNETES);
  }

  @Test
  public void singleDnsFailureThenSuccessKeepsKubernetesJoin() throws Exception {
    int[] attempts = {0};
    InetAddress nonLoopback = InetAddress.getByName("8.8.8.8");
    assertEquals(
        HazelcastDiscoveryMode.decide(
            "hazelcast-service",
            name -> {
              if (attempts[0]++ == 0) {
                throw new UnknownHostException(name);
              }
              return new InetAddress[] {nonLoopback};
            }),
        Join.KUBERNETES);
    assertEquals(attempts[0], 2);
  }

  @Test
  public void dnsFailureInsideKubernetesKeepsJoin() {
    assertEquals(
        HazelcastDiscoveryMode.decide(
            "hazelcast-service",
            name -> {
              throw new UnknownHostException(name);
            },
            "10.0.0.1",
            () -> {}),
        Join.KUBERNETES);
  }
}
