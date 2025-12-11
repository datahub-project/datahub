/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.iceberg.catalog;

import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

interface FileIOFactory {
  FileIO createIO(
      String platformInstance, PoliciesConfig.Privilege privilege, Set<String> locations);

  FileIO createIO(
      String platformInstance, PoliciesConfig.Privilege privilege, TableMetadata tableMetadata);
}
