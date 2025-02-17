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
