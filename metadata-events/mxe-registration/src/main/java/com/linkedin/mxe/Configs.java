package com.linkedin.mxe;

import com.linkedin.pegasus2avro.mxe.FailedMetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.MetadataAuditEvent;
import com.linkedin.pegasus2avro.mxe.MetadataChangeEvent;
import com.linkedin.pegasus2avro.mxe.MetadataGraphEvent;
import com.linkedin.pegasus2avro.mxe.MetadataSearchEvent;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


public class Configs {

  public static final Map<String, String> FABRIC_SCHEMA_REGISTRY_MAP =
      Collections.unmodifiableMap(new HashMap<String, String>() {
        {
          put("ei", "http://1.schemaregistry.ei4.atd.int.linkedin.com:10252");
          put("corp", "http://1.schemaregistry.corp-lca1.atd.corp.linkedin.com:10252");
        }
      });

  public static final Map<String, Schema> TOPIC_SCHEMA_MAP = Collections.unmodifiableMap(new HashMap<String, Schema>() {
    {
      put(Topics.METADATA_AUDIT_EVENT, MetadataAuditEvent.SCHEMA$);
      put(Topics.METADATA_CHANGE_EVENT, MetadataChangeEvent.SCHEMA$);
      put(Topics.FAILED_METADATA_CHANGE_EVENT, FailedMetadataChangeEvent.SCHEMA$);
      put(Topics.METADATA_GRAPH_EVENT, MetadataGraphEvent.SCHEMA$);
      put(Topics.METADATA_SEARCH_EVENT, MetadataSearchEvent.SCHEMA$);

      put(Topics.DEV_METADATA_AUDIT_EVENT, MetadataAuditEvent.SCHEMA$);
      put(Topics.DEV_METADATA_CHANGE_EVENT, MetadataChangeEvent.SCHEMA$);
      put(Topics.DEV_FAILED_METADATA_CHANGE_EVENT, FailedMetadataChangeEvent.SCHEMA$);
    }
  });

  public static final Map<String, Class<? extends SpecificRecord>> TOPIC_SCHEMA_CLASS_MAP =
      Collections.unmodifiableMap(new HashMap<String, Class<? extends SpecificRecord>>() {
        {
          // Aspect-specific MCE topic to schema.
          // CorpGroupUrn
          put(Topics.METADATA_AUDIT_EVENT_CORPGROUP_CORPGROUPINFO,
              com.linkedin.pegasus2avro.mxe.corpGroup.corpGroupInfo.MetadataAuditEvent.class);
          // CorpUserUrn
          put(Topics.METADATA_AUDIT_EVENT_CORPUSER_CORPUSERINFO,
              com.linkedin.pegasus2avro.mxe.corpuser.corpUserInfo.MetadataAuditEvent.class);
          put(Topics.METADATA_AUDIT_EVENT_CORPUSER_CORPUSEREDITABLEINFO,
              com.linkedin.pegasus2avro.mxe.corpuser.corpUserEditableInfo.MetadataAuditEvent.class);

          // Aspect-specific MCE topic to schema.
          // CorpGroupUrn
          put(Topics.METADATA_CHANGE_EVENT_CORPGROUP_CORPGROUPINFO,
              com.linkedin.pegasus2avro.mxe.corpGroup.corpGroupInfo.MetadataChangeEvent.class);
          // CorpUserUrn
          put(Topics.METADATA_CHANGE_EVENT_CORPUSER_CORPUSERINFO,
              com.linkedin.pegasus2avro.mxe.corpuser.corpUserInfo.MetadataChangeEvent.class);
          put(Topics.METADATA_CHANGE_EVENT_CORPUSER_CORPUSEREDITABLEINFO,
              com.linkedin.pegasus2avro.mxe.corpuser.corpUserEditableInfo.MetadataChangeEvent.class);

          // Aspect-specific FMCE topic to schema.
          // CorpGroupUrn
          put(Topics.FAILED_METADATA_CHANGE_EVENT_CORPGROUP_CORPGROUPINFO,
              com.linkedin.pegasus2avro.mxe.corpGroup.corpGroupInfo.FailedMetadataChangeEvent.class);
          // CorpUserUrn
          put(Topics.FAILED_METADATA_CHANGE_EVENT_CORPUSER_CORPUSERINFO,
              com.linkedin.pegasus2avro.mxe.corpuser.corpUserInfo.FailedMetadataChangeEvent.class);
          put(Topics.FAILED_METADATA_CHANGE_EVENT_CORPUSER_CORPUSEREDITABLEINFO,
              com.linkedin.pegasus2avro.mxe.corpuser.corpUserEditableInfo.FailedMetadataChangeEvent.class);
        }
      });

  private Configs() {
    // Util class
  }
}