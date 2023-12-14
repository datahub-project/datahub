package com.linkedin.metadata.systemmetadata;

import com.linkedin.mxe.SystemMetadata;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class SystemMetadataEntry {
  SystemMetadata _systemMetadata;
  String _urn;
  String _aspect;
  Long _version;
}
