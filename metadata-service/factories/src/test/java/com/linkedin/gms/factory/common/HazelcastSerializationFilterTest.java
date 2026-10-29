package com.linkedin.gms.factory.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.metadata.config.hazelcast.HazelcastSerializationAllowListContributor;
import com.linkedin.metadata.entity.retention.SimpleRetentionKey;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.search.cache.CacheableSearcher;
import com.linkedin.metadata.search.cache.CachedEntityLineageResult;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.javatuples.Septet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class HazelcastSerializationFilterTest {

  private static final String GRAPHQL_GUAVA = "graphql.com.google.common.";
  private static final String ICEBERG_GUAVA = "org.apache.iceberg.relocated.com.google.common.";

  private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

  @AfterMethod
  public void tearDown() {
    factory.terminateAll();
  }

  @Test
  public void rejectsGuavaMapProxiesFromBundledCopies() {
    InternalSerializationService serialization = filteredSerialization(List.of(), null, null);

    // graphql-java bundles its own Guava; its MapMakerInternalMap serializes as a
    // SerializationProxy whose readObject allocates from a size read off the stream.
    Data graphqlProxy =
        serialization.toData(new graphql.com.google.common.collect.MapMaker().weakKeys().makeMap());
    assertRejected(serialization, graphqlProxy);

    // Same payload with the class names Iceberg's relocated Guava uses. The filter rejects the
    // name before the class is loaded, so the jar need not be on the test classpath.
    assertRejected(serialization, renamePrefix(graphqlProxy, GRAPHQL_GUAVA, ICEBERG_GUAVA));

    Data guavaProxy =
        serialization.toData(new com.google.common.collect.MapMaker().weakKeys().makeMap());
    assertRejected(serialization, guavaProxy);
  }

  @Test
  public void guavaStaysBlockedWhenABroadPrefixIsAllowed() {
    InternalSerializationService serialization =
        filteredSerialization(List.of(), null, "graphql.,com.google.");

    assertRejected(
        serialization,
        serialization.toData(
            new graphql.com.google.common.collect.MapMaker().weakKeys().makeMap()));
  }

  @Test
  public void dataHubTypesRoundTrip() {
    InternalSerializationService serialization = filteredSerialization(List.of(), null, null);

    LineageRelationshipArray relationships = new LineageRelationshipArray();
    relationships.add(new LineageRelationship().setEntity(new CorpuserUrn("user")).setType("t"));
    EntityLineageResult lineage =
        new EntityLineageResult().setRelationships(relationships).setStart(0).setCount(1);

    Map<String, Object> values = new HashMap<>();
    values.put("lineage", new CachedEntityLineageResult(lineage, 1L));
    values.put("retentionKey", new SimpleRetentionKey("urn:li:corpuser:user", "corpUserInfo"));
    values.put(
        "entityCounts",
        new KeyAspectEntityCountResult(
            List.of(new KeyAspectEntityCountEntry("dataset", "datasetKey", 3, 1)),
            List.of("dataset"),
            Instant.ofEpochMilli(1_000L),
            false));
    values.put(
        "searchKey",
        Septet.with(
            "ctx",
            new ArrayList<>(List.of("dataset")),
            "*",
            null,
            null,
            new ArrayList<>(List.of("platform")),
            new CacheableSearcher.QueryPagination(0, 10)));

    for (Map.Entry<String, Object> entry : values.entrySet()) {
      assertEquals(
          serialization.toObject(serialization.toData(entry.getValue())),
          entry.getValue(),
          entry.getKey());
    }
  }

  @Test
  public void contributorsAndPropertiesExtendTheAllowList() {
    InternalSerializationService defaults = filteredSerialization(List.of(), null, null);
    assertRejected(defaults, defaults.toData(new CustomValue("a")));

    HazelcastSerializationAllowListContributor contributor =
        new HazelcastSerializationAllowListContributor() {
          @Override
          public Set<String> classNames() {
            return Set.of(CustomValue.class.getName());
          }
        };
    InternalSerializationService fromContributor =
        filteredSerialization(List.of(contributor), null, null);
    assertEquals(
        fromContributor.toObject(fromContributor.toData(new CustomValue("b"))),
        new CustomValue("b"));

    InternalSerializationService fromProperty =
        filteredSerialization(List.of(), " other.Type , " + CustomValue.class.getName(), null);
    assertEquals(
        fromProperty.toObject(fromProperty.toData(new CustomValue("c"))), new CustomValue("c"));
  }

  private InternalSerializationService filteredSerialization(
      Collection<HazelcastSerializationAllowListContributor> contributors,
      String classNames,
      String packagePrefixes) {
    Config config = new Config();
    HazelcastSerializationFilter.apply(config, contributors, classNames, packagePrefixes);
    HazelcastInstance instance = factory.newHazelcastInstance(config);
    return (InternalSerializationService)
        ((SerializationServiceSupport) instance).getSerializationService();
  }

  private static void assertRejected(InternalSerializationService serialization, Data data) {
    HazelcastSerializationException e =
        expectThrows(HazelcastSerializationException.class, () -> serialization.toObject(data));
    assertTrue(hasCause(e, SecurityException.class), "expected a SecurityException cause: " + e);
  }

  private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (type.isInstance(c)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Rewrites dotted class names that start with {@code from}. Java serialization writes each class
   * name as a modified-UTF8 string preceded by its two-byte length, so the length is patched too.
   */
  private static Data renamePrefix(Data data, String from, String to) {
    byte[] in = data.toByteArray();
    byte[] fromBytes = from.getBytes(StandardCharsets.UTF_8);
    byte[] toBytes = to.getBytes(StandardCharsets.UTF_8);
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    int i = 0;
    boolean renamed = false;
    while (i < in.length) {
      if (i >= 2 && startsWith(in, i, fromBytes)) {
        int len = ((in[i - 2] & 0xff) << 8) | (in[i - 1] & 0xff);
        if (len >= fromBytes.length && i + len <= in.length) {
          byte[] written = out.toByteArray();
          out.reset();
          out.write(written, 0, written.length - 2);
          int newLen = len - fromBytes.length + toBytes.length;
          out.write(newLen >> 8);
          out.write(newLen & 0xff);
          out.write(toBytes, 0, toBytes.length);
          i += fromBytes.length;
          renamed = true;
          continue;
        }
      }
      out.write(in[i]);
      i++;
    }
    assertTrue(renamed, "no class name with prefix " + from);
    return new HeapData(out.toByteArray());
  }

  private static boolean startsWith(byte[] bytes, int offset, byte[] prefix) {
    if (offset + prefix.length > bytes.length) {
      return false;
    }
    for (int j = 0; j < prefix.length; j++) {
      if (bytes[offset + j] != prefix[j]) {
        return false;
      }
    }
    return true;
  }

  private record CustomValue(String value) implements Serializable {}
}
