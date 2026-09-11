package io.datahubproject.openlineage.dataset;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.Operation;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.schema.SchemaMetadata;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.Singular;
import lombok.ToString;

@Getter
@Builder
@Setter
@ToString
public class DatahubDataset {
  DatasetUrn urn;
  SchemaMetadata schemaMetadata;
  UpstreamLineage lineage;
  // Timeseries aspects, so each point stands on its own and none of them replaces another. They
  // are held as lists because coalescing merges several events into one job: keeping a single
  // value would silently discard every point but the last.
  @Singular List<Operation> operations;
  @Singular List<DatasetProfile> profiles;
  GlobalTags tags;
  Ownership ownership;
  DatasetProperties properties;
}
