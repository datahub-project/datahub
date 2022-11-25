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

  private static final Map<String, Schema> schemas = new HashMap<>();
  static {
    schemas.put(MetadataChangeEvent.class.getName(), MetadataChangeEvent.getClassSchema());
    schemas.put(FailedMetadataChangeEvent.class.getName(), FailedMetadataChangeEvent.getClassSchema());
    schemas.put(MetadataAuditEvent.class.getName(), MetadataAuditEvent.getClassSchema());

    schemas.put(MetadataChangeProposal.class.getName(), MetadataChangeProposal.getClassSchema());
    schemas.put(FailedMetadataChangeProposal.class.getName(), FailedMetadataChangeProposal.getClassSchema());
    schemas.put(MetadataChangeLog.class.getName(), MetadataChangeLog.getClassSchema());
  }
  private static InMemorySchemaRegistry single_instance = null;

  public static InMemorySchemaRegistry getInstance()
  {
    if (single_instance == null)
      single_instance = new InMemorySchemaRegistry();

    return single_instance;
  }

  public Schema getSchema(String topic) {
    return schemas.get(topic);
  }
}
