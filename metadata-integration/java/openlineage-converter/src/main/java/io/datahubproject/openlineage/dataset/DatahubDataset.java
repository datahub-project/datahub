package io.datahubproject.openlineage.dataset;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.Operation;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.schema.SchemaMetadata;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Builder
@Setter
@ToString
public class DatahubDataset {
  DatasetUrn urn;
  SchemaMetadata schemaMetadata;
  UpstreamLineage lineage;
  // Timeseries aspects: each event appends a point rather than overwriting, so no patch handling
  // is needed for these two.
  Operation operation;
  DatasetProfile profile;
  GlobalTags tags;
  Ownership ownership;
  DatasetProperties properties;
}
