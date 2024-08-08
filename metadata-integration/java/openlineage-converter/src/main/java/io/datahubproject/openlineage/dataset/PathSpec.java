package io.datahubproject.openlineage.dataset;

import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@Getter
@Setter
@ToString
public class PathSpec {
  final String alias;
  final String platform;
  @Builder.Default final Optional<String> env = Optional.empty();
  final List<String> pathSpecList;
  @Builder.Default final Optional<String> platformInstance = Optional.empty();
}
