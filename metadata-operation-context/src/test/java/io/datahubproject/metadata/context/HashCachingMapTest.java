package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class HashCachingMapTest {

  @Test
  public void testHashCodeComputedOnce() {
    AtomicInteger hashCalls = new AtomicInteger();
    Map<String, String> counting =
        new HashMap<>(Map.of("k", "v")) {
          @Override
          public int hashCode() {
            hashCalls.incrementAndGet();
            return super.hashCode();
          }
        };
    HashCachingMap<String, String> map = new HashCachingMap<>(counting);

    int first = map.hashCode();
    assertEquals(map.hashCode(), first);
    assertEquals(map.hashCode(), first);

    assertEquals(hashCalls.get(), 1);
  }

  @Test
  public void testEqualityIsByContent() {
    HashCachingMap<String, Integer> a = new HashCachingMap<>(Map.of("k", 1));
    HashCachingMap<String, Integer> sameContent =
        new HashCachingMap<>(new HashMap<>(Map.of("k", 1)));
    HashCachingMap<String, Integer> different = new HashCachingMap<>(Map.of("k", 2));

    assertEquals(a, sameContent);
    assertEquals(a.hashCode(), sameContent.hashCode());
    assertNotEquals(a, different);
  }

  @Test
  public void testFollowsMapContract() {
    Map<String, Integer> plain = Map.of("k", 1, "j", 2);
    HashCachingMap<String, Integer> wrapped = new HashCachingMap<>(new HashMap<>(plain));

    assertEquals(wrapped, plain);
    assertEquals(plain, wrapped);
    assertEquals(wrapped.hashCode(), plain.hashCode());
  }

  @Test
  public void testDelegatesMapOperations() {
    HashCachingMap<String, Integer> map = new HashCachingMap<>(Map.of("k", 1));

    assertEquals(map.get("k"), Integer.valueOf(1));
    assertEquals(map.getOrDefault("missing", 0), Integer.valueOf(0));
    assertEquals(map.size(), 1);
  }
}
