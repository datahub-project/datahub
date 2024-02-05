package io.datahubproject.test.models;

import com.fasterxml.jackson.annotation.JsonGetter;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DatasetAnonymized extends Anonymized {

  public Set<String> upstreams;
  public String id;
  public String origin;
  public String platform;
  public boolean removed;
  public Set<String> browsePaths;

  @JsonGetter("id")
  public String getId() {
    return Optional.ofNullable(id).map(Anonymized::hashFunction).orElse(null);
  }

  @JsonGetter("platform")
  public String getPlatform() {
    return Optional.ofNullable(platform).map(p -> Anonymized.anonymizeLast(p, ":")).orElse(null);
  }

  @JsonGetter("upstreams")
  public Set<String> getUpstreams() {
    return Optional.ofNullable(upstreams).orElse(Set.of()).stream()
        .map(Anonymized::anonymizeUrn)
        .collect(Collectors.toSet());
  }

  @JsonGetter("browsePaths")
  public Set<String> getBrowsePaths() {
    return Optional.ofNullable(browsePaths).orElse(Set.of()).stream()
        .map(
            p ->
                Arrays.stream(p.split("/"))
                    .map(Anonymized::hashFunction)
                    .collect(Collectors.joining("/")))
        .collect(Collectors.toSet());
  }
}
