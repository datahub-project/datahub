/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.client.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.EventFormatter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroSerializer {

  private final Schema _recordSchema;
  private final Schema _genericAspectSchema;
  private final Schema _changeTypeEnumSchema;
  private final EventFormatter _eventFormatter;

  public AvroSerializer() throws IOException {
    _recordSchema =
        new Schema.Parser()
            .parse(
                this.getClass()
                    .getClassLoader()
                    .getResourceAsStream("MetadataChangeProposal.avsc"));
    _genericAspectSchema = this._recordSchema.getField("aspect").schema().getTypes().get(1);
    _changeTypeEnumSchema = this._recordSchema.getField("changeType").schema();
    _eventFormatter = new EventFormatter(EventFormatter.Format.PEGASUS_JSON);
  }

  @VisibleForTesting
  Schema getRecordSchema() {
    return _recordSchema;
  }

  public GenericRecord serialize(@SuppressWarnings("rawtypes") MetadataChangeProposalWrapper mcpw)
      throws IOException {
    return serialize(_eventFormatter.convert(mcpw));
  }

  public GenericRecord serialize(MetadataChangeProposal mcp) throws IOException {
    GenericRecord genericRecord = new GenericData.Record(this._recordSchema);
    genericRecord.put("entityUrn", mcp.getEntityUrn().toString());
    GenericRecord genericAspect = new GenericData.Record(this._genericAspectSchema);
    genericAspect.put("contentType", "application/json");
    genericAspect.put("value", mcp.getAspect().getValue().asByteBuffer());
    genericRecord.put("aspect", genericAspect);
    genericRecord.put("aspectName", mcp.getAspectName());
    genericRecord.put("entityType", mcp.getEntityType());
    genericRecord.put(
        "changeType", new GenericData.EnumSymbol(_changeTypeEnumSchema, mcp.getChangeType()));
    return genericRecord;
  }
}
