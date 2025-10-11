package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LinkSettingsInput;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Clock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LinkUtilsTest {

  private static final String TEST_ENTITY_URN = "urn:li:dataset:(test,test,test)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:testUser";
  private static final String TEST_URL = "https://example.com";
  private static final String TEST_LABEL = "Test Link";
  private static final String NEW_URL = "https://new-example.com";
  private static final String NEW_LABEL = "New Test Link";

  private EntityService<ChangeItemImpl> mockEntityService;
  private OperationContext mockOpContext;
  private Urn resourceUrn;
  private Urn actorUrn;

  @BeforeMethod
  public void setup() throws Exception {
    mockEntityService = Mockito.mock(EntityService.class);
    mockOpContext = Mockito.mock(OperationContext.class);
    resourceUrn = UrnUtils.getUrn(TEST_ENTITY_URN);
    actorUrn = UrnUtils.getUrn(TEST_ACTOR_URN);

    // Mock entity exists by default
    Mockito.when(mockEntityService.exists(eq(mockOpContext), eq(resourceUrn), eq(true)))
        .thenReturn(true);
  }

  // Helper method to create test institutional memory
  private InstitutionalMemory createTestInstitutionalMemory(String url, String label)
      throws Exception {
    InstitutionalMemory memory = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();

    InstitutionalMemoryMetadata link = new InstitutionalMemoryMetadata();
    link.setUrl(new Url(url));
    link.setDescription(label);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn);
    auditStamp.setTime(Clock.systemUTC().millis());
    link.setCreateStamp(auditStamp);

    elements.add(link);
    memory.setElements(elements);

    return memory;
  }

  // Helper method to create test settings
  private LinkSettingsInput createTestSettings(boolean showInAssetPreview) {
    LinkSettingsInput settings = new LinkSettingsInput();
    settings.setShowInAssetPreview(showInAssetPreview);
    return settings;
  }

  @Test
  public void testAddLinkSuccess() throws Exception {
    // Setup
    InstitutionalMemory emptyMemory = new InstitutionalMemory();

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(emptyMemory);

      AuditStamp mockAuditStamp = new AuditStamp();
      mockAuditStamp.setActor(actorUrn);
      mockAuditStamp.setTime(Clock.systemUTC().millis());

      entityUtilsMock
          .when(() -> EntityUtils.getAuditStamp(eq(actorUrn)))
          .thenReturn(mockAuditStamp);

      // Execute
      LinkUtils.addLink(
          mockOpContext, TEST_URL, TEST_LABEL, resourceUrn, actorUrn, null, mockEntityService);

      // Verify
      assertTrue(emptyMemory.hasElements());
      assertEquals(emptyMemory.getElements().size(), 1);

      InstitutionalMemoryMetadata addedLink = emptyMemory.getElements().get(0);
      assertEquals(addedLink.getUrl().toString(), TEST_URL);
      assertEquals(addedLink.getDescription(), TEST_LABEL);
      assertNotNull(addedLink.getCreateStamp());
    }
  }

  @Test
  public void testAddLinkWithSettings() throws Exception {
    // Setup
    InstitutionalMemory emptyMemory = new InstitutionalMemory();
    LinkSettingsInput settings = createTestSettings(true);

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(emptyMemory);

      AuditStamp mockAuditStamp = new AuditStamp();
      mockAuditStamp.setActor(actorUrn);
      mockAuditStamp.setTime(Clock.systemUTC().millis());

      entityUtilsMock
          .when(() -> EntityUtils.getAuditStamp(eq(actorUrn)))
          .thenReturn(mockAuditStamp);

      // Execute
      LinkUtils.addLink(
          mockOpContext, TEST_URL, TEST_LABEL, resourceUrn, actorUrn, settings, mockEntityService);

      // Verify
      InstitutionalMemoryMetadata addedLink = emptyMemory.getElements().get(0);
      assertNotNull(addedLink.getSettings());
      assertTrue(addedLink.getSettings().isShowInAssetPreview());
    }
  }

  @Test
  public void testAddLinkDuplicateThrowsException() throws Exception {
    // Setup
    InstitutionalMemory existingMemory = createTestInstitutionalMemory(TEST_URL, TEST_LABEL);

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(existingMemory);

      // Execute & Verify
      assertThrows(
          IllegalArgumentException.class,
          () -> {
            LinkUtils.addLink(
                mockOpContext,
                TEST_URL,
                TEST_LABEL,
                resourceUrn,
                actorUrn,
                null,
                mockEntityService);
          });
    }
  }

  @Test
  public void testUpdateLinkSuccess() throws Exception {
    // Setup
    InstitutionalMemory existingMemory = createTestInstitutionalMemory(TEST_URL, TEST_LABEL);

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(existingMemory);

      AuditStamp mockUpdateStamp = new AuditStamp();
      mockUpdateStamp.setActor(actorUrn);
      mockUpdateStamp.setTime(Clock.systemUTC().millis() + 1000);

      entityUtilsMock
          .when(() -> EntityUtils.getAuditStamp(eq(actorUrn)))
          .thenReturn(mockUpdateStamp);

      // Execute
      LinkUtils.updateLink(
          mockOpContext,
          TEST_URL,
          TEST_LABEL,
          NEW_URL,
          NEW_LABEL,
          resourceUrn,
          actorUrn,
          null,
          mockEntityService);

      // Verify
      assertEquals(existingMemory.getElements().size(), 1);
      InstitutionalMemoryMetadata updatedLink = existingMemory.getElements().get(0);
      assertEquals(updatedLink.getUrl().toString(), NEW_URL);
      assertEquals(updatedLink.getDescription(), NEW_LABEL);
      assertNotNull(updatedLink.getUpdateStamp());
    }
  }

  @Test
  public void testUpdateLinkNonExistentThrowsException() throws Exception {
    // Setup
    InstitutionalMemory emptyMemory = new InstitutionalMemory();

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(emptyMemory);

      // Execute & Verify
      assertThrows(
          IllegalArgumentException.class,
          () -> {
            LinkUtils.updateLink(
                mockOpContext,
                TEST_URL,
                TEST_LABEL,
                NEW_URL,
                NEW_LABEL,
                resourceUrn,
                actorUrn,
                null,
                mockEntityService);
          });
    }
  }

  @Test
  public void testUpdateLinkToDuplicateThrowsException() throws Exception {
    // Setup - create memory with two links
    InstitutionalMemory memory = new InstitutionalMemory();
    InstitutionalMemoryMetadataArray elements = new InstitutionalMemoryMetadataArray();

    // First link
    InstitutionalMemoryMetadata link1 = new InstitutionalMemoryMetadata();
    link1.setUrl(new Url(TEST_URL));
    link1.setDescription(TEST_LABEL);
    elements.add(link1);

    // Second link (target for update)
    InstitutionalMemoryMetadata link2 = new InstitutionalMemoryMetadata();
    link2.setUrl(new Url(NEW_URL));
    link2.setDescription(NEW_LABEL);
    elements.add(link2);

    memory.setElements(elements);

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(memory);

      // Execute & Verify - try to update second link to match first link
      assertThrows(
          IllegalArgumentException.class,
          () -> {
            LinkUtils.updateLink(
                mockOpContext,
                NEW_URL,
                NEW_LABEL,
                TEST_URL,
                TEST_LABEL,
                resourceUrn,
                actorUrn,
                null,
                mockEntityService);
          });
    }
  }

  @Test
  public void testUpsertLinkInsert() throws Exception {
    // Setup
    InstitutionalMemory emptyMemory = new InstitutionalMemory();

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(emptyMemory);

      AuditStamp mockAuditStamp = new AuditStamp();
      mockAuditStamp.setActor(actorUrn);
      mockAuditStamp.setTime(Clock.systemUTC().millis());

      entityUtilsMock
          .when(() -> EntityUtils.getAuditStamp(eq(actorUrn)))
          .thenReturn(mockAuditStamp);

      // Execute
      LinkUtils.upsertLink(
          mockOpContext, TEST_URL, TEST_LABEL, resourceUrn, actorUrn, null, mockEntityService);

      // Verify - should act like add
      assertTrue(emptyMemory.hasElements());
      assertEquals(emptyMemory.getElements().size(), 1);

      InstitutionalMemoryMetadata addedLink = emptyMemory.getElements().get(0);
      assertEquals(addedLink.getUrl().toString(), TEST_URL);
      assertEquals(addedLink.getDescription(), TEST_LABEL);
    }
  }

  @Test
  public void testUpsertLinkUpdate() throws Exception {
    // Setup
    InstitutionalMemory existingMemory = createTestInstitutionalMemory(TEST_URL, TEST_LABEL);

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(existingMemory);

      AuditStamp mockUpdateStamp = new AuditStamp();
      mockUpdateStamp.setActor(actorUrn);
      mockUpdateStamp.setTime(Clock.systemUTC().millis() + 1000);

      entityUtilsMock
          .when(() -> EntityUtils.getAuditStamp(eq(actorUrn)))
          .thenReturn(mockUpdateStamp);

      // Execute
      LinkUtils.upsertLink(
          mockOpContext, TEST_URL, TEST_LABEL, resourceUrn, actorUrn, null, mockEntityService);

      // Verify - should act like update (same URL and label, so it updates existing)
      assertEquals(existingMemory.getElements().size(), 1);
      InstitutionalMemoryMetadata link = existingMemory.getElements().get(0);
      assertEquals(link.getUrl().toString(), TEST_URL);
      assertEquals(link.getDescription(), TEST_LABEL);
      assertNotNull(link.getUpdateStamp());
    }
  }

  @Test
  public void testRemoveLinkWithLabel() throws Exception {
    // Setup
    InstitutionalMemory existingMemory = createTestInstitutionalMemory(TEST_URL, TEST_LABEL);

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(existingMemory);

      // Execute
      LinkUtils.removeLink(
          mockOpContext, TEST_URL, TEST_LABEL, resourceUrn, actorUrn, mockEntityService);

      // Verify
      assertTrue(existingMemory.getElements().isEmpty());
    }
  }

  @Test
  public void testRemoveLinkWithoutLabel() throws Exception {
    // Setup
    InstitutionalMemory existingMemory = createTestInstitutionalMemory(TEST_URL, TEST_LABEL);

    try (MockedStatic<EntityUtils> entityUtilsMock = Mockito.mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(
              () ->
                  EntityUtils.getAspectFromEntity(
                      eq(mockOpContext),
                      eq(TEST_ENTITY_URN),
                      eq(INSTITUTIONAL_MEMORY_ASPECT_NAME),
                      eq(mockEntityService),
                      any(InstitutionalMemory.class)))
          .thenReturn(existingMemory);

      // Execute
      LinkUtils.removeLink(mockOpContext, TEST_URL, null, resourceUrn, actorUrn, mockEntityService);

      // Verify
      assertTrue(existingMemory.getElements().isEmpty());
    }
  }

  @Test
  public void testIsAuthorizedToUpdateLinksAllow() {
    // Setup
    QueryContext mockContext = getMockAllowContext();

    // Execute
    boolean result = LinkUtils.isAuthorizedToUpdateLinks(mockContext, resourceUrn);

    // Verify
    assertTrue(result);
  }

  @Test
  public void testIsAuthorizedToUpdateLinksDeny() {
    // Setup
    QueryContext mockContext = getMockDenyContext();

    // Execute
    boolean result = LinkUtils.isAuthorizedToUpdateLinks(mockContext, resourceUrn);

    // Verify
    assertFalse(result);
  }

  @Test
  public void testValidateAddRemoveInputSuccess() throws Exception {
    // Execute - should not throw exception
    LinkUtils.validateAddRemoveInput(mockOpContext, TEST_URL, resourceUrn, mockEntityService);
  }

  @Test
  public void testValidateAddRemoveInputNonExistentEntity() throws Exception {
    // Setup
    Mockito.when(mockEntityService.exists(eq(mockOpContext), eq(resourceUrn), eq(true)))
        .thenReturn(false);

    // Execute & Verify
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          LinkUtils.validateAddRemoveInput(mockOpContext, TEST_URL, resourceUrn, mockEntityService);
        });
  }

  @Test
  public void testValidateUpdateInputSuccess() throws Exception {
    // Execute - should not throw exception
    LinkUtils.validateUpdateInput(mockOpContext, TEST_URL, NEW_URL, resourceUrn, mockEntityService);
  }
}
