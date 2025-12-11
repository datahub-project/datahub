/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openlineage.dataset;

import com.linkedin.common.urn.DatasetUrn;
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
}
