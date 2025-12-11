/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.mxe;

import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.MetadataAuditEvent;
import com.linkedin.pegasus2avro.mxe.MetadataChangeEvent;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;

public class Configs {

  public static final Map<String, String> FABRIC_SCHEMA_REGISTRY_MAP =
      Collections.unmodifiableMap(
          new HashMap<String, String>() {
            {
              put("ei", "http://1.schemaregistry.ei4.atd.int.linkedin.com:10252");
              put("corp", "http://1.schemaregistry.corp-lca1.atd.corp.linkedin.com:10252");
            }
          });

  public static final Map<String, Schema> TOPIC_SCHEMA_MAP =
      Collections.unmodifiableMap(
          new HashMap<String, Schema>() {
            {
              put(Topics.METADATA_AUDIT_EVENT, MetadataAuditEvent.SCHEMA$);
              put(Topics.METADATA_CHANGE_EVENT, MetadataChangeEvent.SCHEMA$);
              put(Topics.FAILED_METADATA_CHANGE_EVENT, FailedMetadataChangeEvent.SCHEMA$);

              put(Topics.DEV_METADATA_AUDIT_EVENT, MetadataAuditEvent.SCHEMA$);
              put(Topics.DEV_METADATA_CHANGE_EVENT, MetadataChangeEvent.SCHEMA$);
              put(Topics.DEV_FAILED_METADATA_CHANGE_EVENT, FailedMetadataChangeEvent.SCHEMA$);
            }
          });

  private Configs() {
    // Util class
  }
}
