package datahub.spark.model;

import datahub.spark.model.dataset.SparkDataset;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
public class DatasetLineage {

  private final Set<SparkDataset> sources = new HashSet<>();

  @Getter private final String callSiteShort;

  @Getter private final String plan;

  @Getter private final SparkDataset sink;

  public void addSource(SparkDataset source) {
    sources.add(source);
  }

  public Set<SparkDataset> getSources() {
    return Collections.unmodifiableSet(sources);
  }
}
