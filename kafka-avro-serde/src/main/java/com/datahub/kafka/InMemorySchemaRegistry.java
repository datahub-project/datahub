package com.datahub.kafka;

import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeProposal;
import com.linkedin.pegasus2avro.mxe.MetadataAuditEvent;
import com.linkedin.pegasus2avro.mxe.MetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.MetadataChangeLog;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import com.linkedin.pegasus2avro.mxe.MetadataChangeProposal;

public class InMemorySchemaRegistry {

  private static final Map<String, Schema> SCHEMAS = new HashMap<>();
  static {
    SCHEMAS.put(MetadataChangeEvent.class.getName(), MetadataChangeEvent.getClassSchema());
    SCHEMAS.put(FailedMetadataChangeEvent.class.getName(), FailedMetadataChangeEvent.getClassSchema());
    SCHEMAS.put(MetadataAuditEvent.class.getName(), MetadataAuditEvent.getClassSchema());

    SCHEMAS.put(MetadataChangeProposal.class.getName(), MetadataChangeProposal.getClassSchema());
    SCHEMAS.put(FailedMetadataChangeProposal.class.getName(), FailedMetadataChangeProposal.getClassSchema());
    SCHEMAS.put(MetadataChangeLog.class.getName(), MetadataChangeLog.getClassSchema());
  }
  private static InMemorySchemaRegistry instance = null;

  public static InMemorySchemaRegistry getInstance() {
    if (instance == null) {
      instance = new InMemorySchemaRegistry();
    }
    return instance;
  }

  public Schema getSchema(String topic) {
    return SCHEMAS.get(topic);
  }
}
