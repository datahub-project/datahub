package io.datahubproject.openlineage.dataset;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Operation;
import com.linkedin.common.Ownership;
import com.linkedin.common.Siblings;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
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
  DatasetProperties datasetProperties;
  DataPlatformInstance dataPlatformInstance;
  DatasetProfile datasetProfile;
  Operation operation;
  Ownership ownership;
  GlobalTags globalTags;
  SubTypes subTypes;
  Siblings siblings;
  Status status;
  UpstreamLineage lineage;
}
