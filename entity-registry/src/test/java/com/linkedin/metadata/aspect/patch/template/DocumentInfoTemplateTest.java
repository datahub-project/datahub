package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.knowledge.RelatedAsset;
import com.linkedin.knowledge.RelatedAssetArray;
import com.linkedin.knowledge.RelatedDocument;
import com.linkedin.knowledge.RelatedDocumentArray;
import com.linkedin.metadata.aspect.patch.template.document.DocumentInfoTemplate;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatch;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DocumentInfoTemplateTest {

  private static final DocumentInfoTemplate TEMPLATE = new DocumentInfoTemplate();
  private static final String INGESTION_ACTOR = "urn:li:corpuser:ingestion";
  private static final String USER_ASSET =
      "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.orders,PROD)";
  private static final String CONNECTOR_ASSET =
      "urn:li:dataset:(urn:li:dataPlatform:postgres,appdb.public.order_events,PROD)";
  private static final String USER_DOCUMENT = "urn:li:document:runbook";
  private static final String PARENT_DOCUMENT = "urn:li:document:handbook";

  private static JsonObjectBuilder op(String op, String path) {
    return Json.createObjectBuilder().add("op", op).add("path", path);
  }

  private static JsonObjectBuilder auditStamp(long time) {
    return Json.createObjectBuilder().add("time", time).add("actor", INGESTION_ACTOR);
  }

  /** The fields a connector owns, as the Python DocumentPatchBuilder emits them. */
  private static JsonArrayBuilder connectorFields(String text) {
    return Json.createArrayBuilder()
        .add(op("add", "/title").add("value", "Onboarding"))
        .add(op("add", "/contents").add("value", Json.createObjectBuilder().add("text", text)))
        .add(
            op("add", "/status").add("value", Json.createObjectBuilder().add("state", "PUBLISHED")))
        .add(
            op("add", "/source")
                .add(
                    "value",
                    Json.createObjectBuilder()
                        .add("sourceType", "EXTERNAL")
                        .add("externalUrl", "https://example.com/onboarding")
                        .add("externalId", "onboarding")))
        .add(op("add", "/created").add("value", auditStamp(1000L)))
        .add(op("add", "/lastModified").add("value", auditStamp(2000L)))
        .add(op("add", "/customProperties/space").add("value", "ENG"))
        .add(op("remove", "/parentDocument"));
  }

  private static JsonPatch patch(JsonArrayBuilder ops) {
    return Json.createPatch(ops.build());
  }

  private static List<String> relatedAssets(DocumentInfo info) {
    return info.getRelatedAssets().stream()
        .map(asset -> asset.getAsset().toString())
        .sorted()
        .collect(Collectors.toList());
  }

  private static DocumentInfo existingDocumentWithUserLinks() {
    AuditStamp stamp = new AuditStamp().setActor(UrnUtils.getUrn(INGESTION_ACTOR)).setTime(1000L);
    return new DocumentInfo()
        .setTitle("Old title")
        .setStatus(new DocumentStatus().setState(DocumentState.PUBLISHED))
        .setContents(new DocumentContents().setText("old text"))
        .setCreated(stamp)
        .setLastModified(stamp)
        .setRelatedAssets(
            new RelatedAssetArray(new RelatedAsset().setAsset(UrnUtils.getUrn(USER_ASSET))))
        .setRelatedDocuments(
            new RelatedDocumentArray(
                new RelatedDocument().setDocument(UrnUtils.getUrn(USER_DOCUMENT))));
  }

  @Test
  public void testPatchCreatesDocumentFromDefault() throws Exception {
    DocumentInfo result =
        TEMPLATE.applyPatch(TEMPLATE.getDefault(), patch(connectorFields("hello")));

    Assert.assertEquals(result.getTitle(), "Onboarding");
    Assert.assertEquals(result.getContents().getText(), "hello");
    Assert.assertEquals(result.getStatus().getState(), DocumentState.PUBLISHED);
    Assert.assertEquals(result.getSource().getExternalId(), "onboarding");
    Assert.assertEquals(result.getCreated().getTime().longValue(), 1000L);
    Assert.assertEquals(result.getLastModified().getTime().longValue(), 2000L);
    Assert.assertEquals(result.getCustomProperties().get("space"), "ENG");
    Assert.assertFalse(result.hasParentDocument());
  }

  @Test
  public void testDefaultIsUnpublished() {
    Assert.assertEquals(TEMPLATE.getDefault().getStatus().getState(), DocumentState.UNPUBLISHED);
  }

  @Test
  public void testReingestionKeepsUserAddedRelatedEntities() throws Exception {
    JsonArrayBuilder ops =
        connectorFields("new text")
            .add(
                op("add", "/relatedAssets/" + CONNECTOR_ASSET)
                    .add("value", Json.createObjectBuilder().add("asset", CONNECTOR_ASSET)));

    DocumentInfo result = TEMPLATE.applyPatch(existingDocumentWithUserLinks(), patch(ops));

    Assert.assertEquals(result.getTitle(), "Onboarding");
    Assert.assertEquals(result.getContents().getText(), "new text");
    // relatedAssets() sorts: CONNECTOR_ASSET's "order_events" sorts before USER_ASSET's "orders".
    Assert.assertEquals(relatedAssets(result), List.of(CONNECTOR_ASSET, USER_ASSET));
    Assert.assertEquals(result.getRelatedDocuments().size(), 1);
    Assert.assertEquals(
        result.getRelatedDocuments().get(0).getDocument().toString(), USER_DOCUMENT);
  }

  @Test
  public void testRemoveRelatedAssetByKeyLeavesOthers() throws Exception {
    DocumentInfo initial = existingDocumentWithUserLinks();
    initial.getRelatedAssets().add(new RelatedAsset().setAsset(UrnUtils.getUrn(CONNECTOR_ASSET)));

    DocumentInfo result =
        TEMPLATE.applyPatch(
            initial,
            patch(
                Json.createArrayBuilder().add(op("remove", "/relatedAssets/" + CONNECTOR_ASSET))));

    Assert.assertEquals(relatedAssets(result), List.of(USER_ASSET));
  }

  @Test
  public void testParentDocumentCanBeSetAndCleared() throws Exception {
    DocumentInfo withParent =
        TEMPLATE.applyPatch(
            existingDocumentWithUserLinks(),
            patch(
                Json.createArrayBuilder()
                    .add(
                        op("add", "/parentDocument")
                            .add(
                                "value",
                                Json.createObjectBuilder().add("document", PARENT_DOCUMENT)))));
    Assert.assertEquals(withParent.getParentDocument().getDocument().toString(), PARENT_DOCUMENT);

    DocumentInfo cleared =
        TEMPLATE.applyPatch(
            withParent, patch(Json.createArrayBuilder().add(op("remove", "/parentDocument"))));
    Assert.assertFalse(cleared.hasParentDocument());
  }

  @Test
  public void testRejectsOtherAspectTypes() {
    DocumentInfo info = TEMPLATE.getDefault();
    Assert.assertSame(TEMPLATE.getSubtype(info), info);
    Assert.assertThrows(
        ClassCastException.class, () -> TEMPLATE.getSubtype(new DocumentContents().setText("")));
  }
}
