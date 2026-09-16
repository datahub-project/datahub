package com.linkedin.gms.factory.entity;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.lock.EntityWriteLock;
import com.linkedin.metadata.entity.lock.HazelcastEntityWriteLock;
import com.linkedin.metadata.entity.lock.NoOpEntityWriteLock;
import org.testng.annotations.Test;

public class EntityWriteLockFactoryTest {

  private final EntityWriteLockFactory factory = new EntityWriteLockFactory();

  private ConfigurationProvider providerFor(String backend) {
    EbeanConfiguration ebean = EbeanConfiguration.builder().entityWriteLockBackend(backend).build();
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.getEbean()).thenReturn(ebean);
    return provider;
  }

  @Test
  public void noneBackendReturnsNoOp() {
    EntityWriteLock lock =
        factory.entityWriteLock(providerFor("none"), mock(HazelcastInstance.class));
    assertTrue(lock instanceof NoOpEntityWriteLock);
  }

  @Test
  public void unrecognizedBackendReturnsNoOp() {
    // Any value other than none|hazelcast (e.g. the removed "db") degrades to no gate.
    EntityWriteLock lock =
        factory.entityWriteLock(providerFor("db"), mock(HazelcastInstance.class));
    assertTrue(lock instanceof NoOpEntityWriteLock);
  }

  @Test
  public void hazelcastBackendReturnsHazelcastLock() {
    HazelcastInstance hz = mock(HazelcastInstance.class);
    when(hz.getMap(anyString())).thenReturn(mock(IMap.class));
    EntityWriteLock lock = factory.entityWriteLock(providerFor("hazelcast"), hz);
    assertTrue(lock instanceof HazelcastEntityWriteLock);
  }

  @Test
  public void hazelcastBackendWithoutInstanceDegradesToNoOp() {
    EntityWriteLock lock = factory.entityWriteLock(providerFor("hazelcast"), null);
    assertTrue(lock instanceof NoOpEntityWriteLock);
  }

  @Test
  public void isActiveDistinguishesRealGateFromNoOp() {
    // isActive() is the signal EntityServiceImpl uses to skip the redundant Postgres advisory lock
    // when a real gate already serializes the URNs; a false positive would double-lock, a false
    // negative would drop serialization.
    HazelcastInstance hz = mock(HazelcastInstance.class);
    when(hz.getMap(anyString())).thenReturn(mock(IMap.class));
    assertTrue(factory.entityWriteLock(providerFor("hazelcast"), hz).isActive());
    assertFalse(factory.entityWriteLock(providerFor("none"), hz).isActive());
  }
}
