package com.linkedin.metadata.aspect.patch.template.document;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Lets ingestion update the fields it owns on a Document without overwriting related assets and
 * related documents that users added in DataHub. Both lists are keyed by URN, so entries can be
 * added or removed one at a time.
 */
public class DocumentInfoTemplate implements ArrayMergingTemplate<DocumentInfo> {

  private static final String RELATED_ASSETS_FIELD_NAME = "relatedAssets";
  private static final String RELATED_ASSET_KEY_FIELD_NAME = "asset";
  private static final String RELATED_DOCUMENTS_FIELD_NAME = "relatedDocuments";
  private static final String RELATED_DOCUMENT_KEY_FIELD_NAME = "document";

  @Override
  public DocumentInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DocumentInfo) {
      return (DocumentInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DocumentInfo");
  }

  @Override
  public Class<DocumentInfo> getTemplateType() {
    return DocumentInfo.class;
  }

  /**
   * Fills the required fields so a patch can create a Document. Unpublished, so a patch that sets
   * no status does not make an empty Document visible.
   */
  @Nonnull
  @Override
  public DocumentInfo getDefault() {
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    return new DocumentInfo()
        .setStatus(new DocumentStatus().setState(DocumentState.UNPUBLISHED))
        .setContents(new DocumentContents().setText(""))
        .setCreated(auditStamp)
        .setLastModified(auditStamp)
        .setCustomProperties(new StringMap());
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode,
            RELATED_ASSETS_FIELD_NAME,
            Collections.singletonList(RELATED_ASSET_KEY_FIELD_NAME));
    return arrayFieldToMap(
        transformedNode,
        RELATED_DOCUMENTS_FIELD_NAME,
        Collections.singletonList(RELATED_DOCUMENT_KEY_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched,
            RELATED_ASSETS_FIELD_NAME,
            Collections.singletonList(RELATED_ASSET_KEY_FIELD_NAME));
    return transformedMapToArray(
        rebasedNode,
        RELATED_DOCUMENTS_FIELD_NAME,
        Collections.singletonList(RELATED_DOCUMENT_KEY_FIELD_NAME));
  }
}
