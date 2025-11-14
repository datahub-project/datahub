package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.knowledge.ParentDocument;
import com.linkedin.knowledge.RelatedAsset;
import com.linkedin.knowledge.RelatedAssetArray;
import com.linkedin.knowledge.RelatedDocument;
import com.linkedin.knowledge.RelatedDocumentArray;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.sql.Timestamp;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DocumentInfoChangeEventGeneratorTest {

  private static final String TEST_URN = "urn:li:document:test-doc";
  private static final String TEST_USER = "urn:li:corpuser:testUser";
  private static final long TEST_TIME = 1234567890000L;

  private DocumentInfoChangeEventGenerator generator;

  @BeforeMethod
  public void setup() {
    generator = new DocumentInfoChangeEventGenerator();
  }

  @Test
  public void testDocumentCreation() throws Exception {
    // Setup - no previous version
    EntityAspect previousAspect = null;
    EntityAspect currentAspect = createEntityAspect(createDocumentInfo("Test Document", "Content"));

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.LIFECYCLE, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.LIFECYCLE);
    assertEquals(event.getOperation(), ChangeOperation.CREATE);
    assertTrue(event.getDescription().contains("Test Document"));
    assertTrue(event.getDescription().contains("created"));
  }

  @Test
  public void testTitleChange() throws Exception {
    // Setup
    DocumentInfo oldDoc = createDocumentInfo("Old Title", "Content");
    DocumentInfo newDoc = createDocumentInfo("New Title", "Content");

    EntityAspect previousAspect = createEntityAspect(oldDoc);
    EntityAspect currentAspect = createEntityAspect(newDoc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.DOCUMENTATION, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.DOCUMENTATION);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertTrue(event.getDescription().contains("title changed"));
    assertTrue(event.getDescription().contains("Old Title"));
    assertTrue(event.getDescription().contains("New Title"));
    assertNotNull(event.getParameters());
    assertEquals(event.getParameters().get("oldTitle"), "Old Title");
    assertEquals(event.getParameters().get("newTitle"), "New Title");
  }

  @Test
  public void testContentChange() throws Exception {
    // Setup
    DocumentInfo oldDoc = createDocumentInfo("Title", "Old Content");
    DocumentInfo newDoc = createDocumentInfo("Title", "New Content");

    EntityAspect previousAspect = createEntityAspect(oldDoc);
    EntityAspect currentAspect = createEntityAspect(newDoc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.DOCUMENTATION, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.DOCUMENTATION);
    assertTrue(event.getDescription().contains("content was modified"));
    // Verify parameters are set
    assertNotNull(event.getParameters());
    assertEquals(event.getParameters().get("oldContent"), "Old Content");
    assertEquals(event.getParameters().get("newContent"), "New Content");
  }

  @Test
  public void testParentDocumentChange() throws Exception {
    // Setup
    Urn oldParentUrn = UrnUtils.getUrn("urn:li:document:old-parent");
    Urn newParentUrn = UrnUtils.getUrn("urn:li:document:new-parent");

    DocumentInfo oldDoc = createDocumentInfo("Title", "Content");
    ParentDocument oldParent = new ParentDocument();
    oldParent.setDocument(oldParentUrn);
    oldDoc.setParentDocument(oldParent);

    DocumentInfo newDoc = createDocumentInfo("Title", "Content");
    ParentDocument newParent = new ParentDocument();
    newParent.setDocument(newParentUrn);
    newDoc.setParentDocument(newParent);

    EntityAspect previousAspect = createEntityAspect(oldDoc);
    EntityAspect currentAspect = createEntityAspect(newDoc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.PARENT, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.PARENT);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertTrue(event.getDescription().contains("moved"));
    assertTrue(event.getDescription().contains(oldParentUrn.toString()));
    assertTrue(event.getDescription().contains(newParentUrn.toString()));
    // Verify parameters are set
    assertNotNull(event.getParameters());
    assertEquals(event.getParameters().get("oldParent"), oldParentUrn.toString());
    assertEquals(event.getParameters().get("newParent"), newParentUrn.toString());
  }

  @Test
  public void testParentDocumentAdded() throws Exception {
    // Setup - document moved from root to having a parent
    Urn parentUrn = UrnUtils.getUrn("urn:li:document:parent");

    DocumentInfo oldDoc = createDocumentInfo("Title", "Content");
    // No parent initially

    DocumentInfo newDoc = createDocumentInfo("Title", "Content");
    ParentDocument parent = new ParentDocument();
    parent.setDocument(parentUrn);
    newDoc.setParentDocument(parent);

    EntityAspect previousAspect = createEntityAspect(oldDoc);
    EntityAspect currentAspect = createEntityAspect(newDoc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.PARENT, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.PARENT);
    assertTrue(event.getDescription().contains("moved to parent"));
    assertTrue(event.getDescription().contains(parentUrn.toString()));
    // Verify parameters are set
    assertNotNull(event.getParameters());
    assertEquals(event.getParameters().get("newParent"), parentUrn.toString());
  }

  @Test
  public void testRelatedAssetAdded() throws Exception {
    // Setup
    Urn assetUrn = UrnUtils.getUrn("urn:li:dataset:test-dataset");

    DocumentInfo oldDoc = createDocumentInfo("Title", "Content");
    DocumentInfo newDoc = createDocumentInfo("Title", "Content");

    RelatedAssetArray assets = new RelatedAssetArray();
    RelatedAsset asset = new RelatedAsset();
    asset.setAsset(assetUrn);
    assets.add(asset);
    newDoc.setRelatedAssets(assets);

    EntityAspect previousAspect = createEntityAspect(oldDoc);
    EntityAspect currentAspect = createEntityAspect(newDoc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.RELATED_ENTITIES, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.RELATED_ENTITIES);
    assertEquals(event.getOperation(), ChangeOperation.ADD);
    assertTrue(event.getDescription().contains("Related asset"));
    assertTrue(event.getDescription().contains("added"));
    assertEquals(event.getModifier(), assetUrn.toString());
  }

  @Test
  public void testRelatedDocumentRemoved() throws Exception {
    // Setup
    Urn docUrn = UrnUtils.getUrn("urn:li:document:related-doc");

    DocumentInfo oldDoc = createDocumentInfo("Title", "Content");
    RelatedDocumentArray oldDocs = new RelatedDocumentArray();
    RelatedDocument relatedDoc = new RelatedDocument();
    relatedDoc.setDocument(docUrn);
    oldDocs.add(relatedDoc);
    oldDoc.setRelatedDocuments(oldDocs);

    DocumentInfo newDoc = createDocumentInfo("Title", "Content");
    // No related documents

    EntityAspect previousAspect = createEntityAspect(oldDoc);
    EntityAspect currentAspect = createEntityAspect(newDoc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.RELATED_ENTITIES, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.RELATED_ENTITIES);
    assertEquals(event.getOperation(), ChangeOperation.REMOVE);
    assertTrue(event.getDescription().contains("Related document"));
    assertTrue(event.getDescription().contains("removed"));
    assertEquals(event.getModifier(), docUrn.toString());
  }

  @Test
  public void testStateChange() throws Exception {
    // Setup
    DocumentInfo oldDoc = createDocumentInfo("Title", "Content");
    DocumentStatus oldStatus = new DocumentStatus();
    oldStatus.setState(DocumentState.UNPUBLISHED);
    oldDoc.setStatus(oldStatus);

    DocumentInfo newDoc = createDocumentInfo("Title", "Content");
    DocumentStatus newStatus = new DocumentStatus();
    newStatus.setState(DocumentState.PUBLISHED);
    newDoc.setStatus(newStatus);

    EntityAspect previousAspect = createEntityAspect(oldDoc);
    EntityAspect currentAspect = createEntityAspect(newDoc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.LIFECYCLE, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 1);

    ChangeEvent event = transaction.getChangeEvents().get(0);
    assertEquals(event.getCategory(), ChangeCategory.LIFECYCLE);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);
    assertTrue(event.getDescription().contains("state changed"));
    assertTrue(event.getDescription().contains("UNPUBLISHED"));
    assertTrue(event.getDescription().contains("PUBLISHED"));
  }

  @Test
  public void testMultipleChangesInSameTransaction() throws Exception {
    // Setup - change both title and content
    DocumentInfo oldDoc = createDocumentInfo("Old Title", "Old Content");
    DocumentInfo newDoc = createDocumentInfo("New Title", "New Content");

    EntityAspect previousAspect = createEntityAspect(oldDoc);
    EntityAspect currentAspect = createEntityAspect(newDoc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.DOCUMENTATION, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 2); // Title + Content changes
  }

  @Test
  public void testNoChanges() throws Exception {
    // Setup - identical documents
    DocumentInfo doc = createDocumentInfo("Title", "Content");

    EntityAspect previousAspect = createEntityAspect(doc);
    EntityAspect currentAspect = createEntityAspect(doc);

    // Execute
    ChangeTransaction transaction =
        generator.getSemanticDiff(
            previousAspect, currentAspect, ChangeCategory.DOCUMENTATION, null, false);

    // Verify
    assertNotNull(transaction);
    assertNotNull(transaction.getChangeEvents());
    assertEquals(transaction.getChangeEvents().size(), 0); // No changes
  }

  // Helper methods

  private DocumentInfo createDocumentInfo(String title, String content) {
    DocumentInfo doc = new DocumentInfo();
    doc.setTitle(title);
    DocumentContents docContent = new DocumentContents();
    docContent.setText(content);
    doc.setContents(docContent);
    return doc;
  }

  private EntityAspect createEntityAspect(DocumentInfo documentInfo) throws Exception {
    EntityAspect aspect = new EntityAspect();
    aspect.setUrn(TEST_URN);
    aspect.setAspect("documentInfo");
    aspect.setVersion(1L);
    aspect.setMetadata(RecordUtils.toJsonString(documentInfo));
    aspect.setCreatedOn(new Timestamp(TEST_TIME));
    aspect.setCreatedBy(TEST_USER);
    return aspect;
  }
}
