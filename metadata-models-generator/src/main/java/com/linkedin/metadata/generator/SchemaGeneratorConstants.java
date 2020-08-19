package com.linkedin.metadata.generator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Constants used in PDL/Rythm to describe event schemas.
 */
public class SchemaGeneratorConstants {
  private SchemaGeneratorConstants() {
  }

  // used in SchemaAnnotationRetriever
  static final String ASPECT = "Aspect";
  static final String DELTA = "Delta";
  static final String ENTITY_URNS = "EntityUrns";

  // used in EventSchemaComposer
  static final String FAILED_METADATA_CHANGE_EVENT = "FailedMetadataChangeEvent";
  static final String FAILED_METADATA_CHANGE_EVENT_PREFIX = "Failed";
  static final String METADATA_AUDIT_EVENT = "MetadataAuditEvent";
  static final String METADATA_CHANGE_EVENT = "MetadataChangeEvent";
  static final String PDL_SUFFIX = ".pdl";
  static final Map<String, String> EVENT_TEMPLATES = Collections.unmodifiableMap(new HashMap<String, String>() {
    {
      put(FAILED_METADATA_CHANGE_EVENT, "FailedMetadataChangeEvent.rythm");
      put(METADATA_AUDIT_EVENT, "MetadataAuditEvent.rythm");
      put(METADATA_CHANGE_EVENT, "MetadataChangeEvent.rythm");
    }
  });
}