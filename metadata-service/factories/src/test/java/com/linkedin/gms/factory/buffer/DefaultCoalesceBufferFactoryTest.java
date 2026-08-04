package com.linkedin.gms.factory.buffer;

import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.buffer.BufferImplementation;
import com.linkedin.metadata.buffer.CaffeineCoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import org.testng.annotations.Test;

public class DefaultCoalesceBufferFactoryTest {

  @Test
  public void testFallsBackToCaffeineWhenHazelcastSelectedButUnavailable() {
    // hazelcast selected but no HazelcastInstance bean available -> must not break; falls back
    // local.
    DefaultCoalesceBufferFactory factory =
        new DefaultCoalesceBufferFactory(BufferImplementation.HAZELCAST, null, null);
    CoalesceBuffer<String, Long> buffer = factory.create("map", "lock", 100);
    assertTrue(buffer instanceof CaffeineCoalesceBuffer);
  }

  @Test
  public void testCaffeineBackendCreatesLocalBuffer() {
    DefaultCoalesceBufferFactory factory =
        new DefaultCoalesceBufferFactory(BufferImplementation.CAFFEINE, null, null);
    assertTrue(factory.create("map", "lock", 100) instanceof CaffeineCoalesceBuffer);
  }
}
