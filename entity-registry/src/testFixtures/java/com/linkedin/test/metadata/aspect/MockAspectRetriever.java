package com.linkedin.test.metadata.aspect;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.test.metadata.aspect.batch.TestSystemAspect;
import com.linkedin.util.Pair;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import org.mockito.Mockito;

public class MockAspectRetriever implements CachingAspectRetriever {
  private final Map<Urn, Map<String, Aspect>> data;
  private final Map<Urn, Map<String, SystemAspect>> systemData = new HashMap<>();
  @Getter @Setter private EntityRegistry entityRegistry;

  public MockAspectRetriever(@Nonnull Map<Urn, List<RecordTemplate>> data) {
    this.data =
        new HashMap<>(
            data.entrySet().stream()
                .map(
                    entry ->
                        Pair.of(
                            entry.getKey(),
                            entry.getValue().stream()
                                .map(
                                    rt -> {
                                      String aspectName =
                                          ((DataMap) rt.schema().getProperties().get("Aspect"))
                                              .get("name")
                                              .toString();
                                      return Pair.of(aspectName, new Aspect(rt.data()));
                                    })
                                .collect(Collectors.toMap(Pair::getKey, Pair::getValue))))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
    for (Map.Entry<Urn, Map<String, Aspect>> urnEntry : this.data.entrySet()) {
      for (Map.Entry<String, Aspect> aspectEntry : urnEntry.getValue().entrySet()) {
        systemData
            .computeIfAbsent(urnEntry.getKey(), urn -> new HashMap<>())
            .computeIfAbsent(
                aspectEntry.getKey(),
                aspectName ->
                    TestSystemAspect.builder()
                        .urn(urnEntry.getKey())
                        .version(0)
                        .systemMetadata(new SystemMetadata().setVersion("1"))
                        .createdOn(Timestamp.from(Instant.now()))
                        .build());
      }
    }
    this.entityRegistry = Mockito.mock(EntityRegistry.class);
  }

  public MockAspectRetriever(
      Urn propertyUrn, StructuredPropertyDefinition definition, Status status) {
    this(Map.of(propertyUrn, List.of(definition, status)));
  }

  public MockAspectRetriever(Urn propertyUrn, StructuredPropertyDefinition definition) {
    this(Map.of(propertyUrn, List.of(definition)));
  }

  @Nonnull
  public Map<Urn, Boolean> entityExists(Set<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    } else {
      return urns.stream().collect(Collectors.toMap(urn -> urn, data::containsKey));
    }
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
      Set<Urn> urns, Set<String> aspectNames) {
    return urns.stream()
        .filter(data::containsKey)
        .map(urn -> Pair.of(urn, data.get(urn)))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
      Map<Urn, Set<String>> urnAspectNames) {
    return urnAspectNames.keySet().stream()
        .filter(systemData::containsKey)
        .map(urn -> Pair.of(urn, systemData.get(urn)))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }
}
