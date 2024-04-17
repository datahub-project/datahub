package com.linkedin.test.metadata.aspect;

import static org.mockito.Mockito.mock;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class MockAspectRetriever implements AspectRetriever {
  private final Map<Urn, Map<String, Aspect>> data;

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
  }

  public MockAspectRetriever(
      Urn propertyUrn, StructuredPropertyDefinition definition, Status status) {
    this(Map.of(propertyUrn, List.of(definition, status)));
  }

  public MockAspectRetriever(Urn propertyUrn, StructuredPropertyDefinition definition) {
    this(Map.of(propertyUrn, List.of(definition)));
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
  public EntityRegistry getEntityRegistry() {
    return mock(EntityRegistry.class);
  }
}
