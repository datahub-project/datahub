package com.linkedin.metadata.aspect.validators;

import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.net.HttpHeaders;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.ConditionalWriteValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.test.metadata.aspect.batch.TestSystemAspect;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ConditionalWriteValidatorTest {
  private EntityRegistry entityRegistry;
  private RetrieverContext mockRetrieverContext;
  private static final List<ChangeType> supportedChangeTypes =
      List.of(
          ChangeType.CREATE,
          ChangeType.CREATE_ENTITY,
          ChangeType.DELETE,
          ChangeType.UPSERT,
          ChangeType.UPDATE,
          ChangeType.RESTATE,
          ChangeType.PATCH);

  private static final AspectPluginConfig validatorConfig =
      AspectPluginConfig.builder()
          .supportedOperations(
              supportedChangeTypes.stream().map(Object::toString).collect(Collectors.toList()))
          .className(ConditionalWriteValidator.class.getName())
          .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
          .enabled(true)
          .build();

  @BeforeTest
  public void init() {
    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testNextVersionSuccess() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case RESTATE:
        case CREATE_ENTITY:
        case CREATE:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "-1"))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "1"))
                  .previousSystemAspect(
                      TestSystemAspect.builder()
                          .urn(testEntityUrn)
                          .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                          .aspectSpec(
                              entityRegistry
                                  .getEntitySpec(testEntityUrn.getEntityType())
                                  .getAspectSpec("status"))
                          // Previous / actual
                          .systemMetadata(new SystemMetadata().setVersion("1"))
                          .build())
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      assertEquals(Set.of(), exceptions, "Expected no exceptions for change type " + changeType);
    }
  }

  @Test
  public void testNoSystemMetadataNextVersionNextVersionSuccess() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case DELETE:
          reset(mockRetrieverContext.getAspectRetriever());
          when(mockRetrieverContext
                  .getAspectRetriever()
                  .getLatestSystemAspects(eq(Map.of(testEntityUrn, Set.of("status")))))
              .thenReturn(
                  Map.of(
                      testEntityUrn,
                      Map.of(
                          "status",
                          TestSystemAspect.builder()
                              .systemMetadata(new SystemMetadata().setVersion("1"))
                              .build())));
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected (cannot delete non-existent -1)
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "1"))
                  .build();
          break;
        case CREATE:
        case CREATE_ENTITY:
          reset(mockRetrieverContext.getAspectRetriever());
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "-1"))
                  .previousSystemAspect(
                      TestSystemAspect.builder()
                          .urn(testEntityUrn)
                          .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                          .aspectSpec(
                              entityRegistry
                                  .getEntitySpec(testEntityUrn.getEntityType())
                                  .getAspectSpec("status"))
                          // Missing previous system metadata, expect fallback to version
                          .version(0)
                          .build())
                  .build();
          break;
        default:
          reset(mockRetrieverContext.getAspectRetriever());
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "1"))
                  .previousSystemAspect(
                      TestSystemAspect.builder()
                          .urn(testEntityUrn)
                          .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                          .aspectSpec(
                              entityRegistry
                                  .getEntitySpec(testEntityUrn.getEntityType())
                                  .getAspectSpec("status"))
                          // Missing previous system metadata, expect fallback to version
                          .version(0)
                          .build())
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      assertEquals(
          Set.of(),
          exceptions,
          String.format(
              "Expected no exceptions for change type %s but found %s", changeType, exceptions));
    }
  }

  @Test
  public void testNoPreviousVersionsLookupSchemaMetadataNextVersionSuccess() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    // Prepare mock lookup based on version
    reset(mockRetrieverContext.getAspectRetriever());
    when(mockRetrieverContext
            .getAspectRetriever()
            .getLatestSystemAspects(eq(Map.of(testEntityUrn, Set.of("status")))))
        .thenReturn(
            Map.of(
                testEntityUrn,
                Map.of(
                    "status",
                    TestSystemAspect.builder()
                        .systemMetadata(
                            new SystemMetadata().setVersion("2")) // expected next version 2
                        .build())));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected is always 1
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "-1"))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      assertEquals(Set.of(), exceptions, "Expected no exceptions for change type " + changeType);
    }
  }

  @Test
  public void testNoPreviousVersionsLookupVersionNextVersionSuccess() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    // Prepare mock lookup based on version
    when(mockRetrieverContext
            .getAspectRetriever()
            .getLatestSystemAspects(eq(Map.of(testEntityUrn, Set.of("status")))))
        .thenReturn(
            Map.of(
                testEntityUrn,
                Map.of(
                    "status",
                    TestSystemAspect.builder()
                        .version(2) // expected next version 2
                        .build())));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected is always 1
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "-1"))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      assertEquals(Set.of(), exceptions, "Expected no exceptions for change type " + changeType);
    }
  }

  @Test
  public void testNextVersionFail() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case DELETE:
          // allow lookup of previous value
          when(mockRetrieverContext
                  .getAspectRetriever()
                  .getLatestSystemAspects(Map.of(testEntityUrn, Set.of("status"))))
              .thenReturn(
                  Map.of(
                      testEntityUrn,
                      Map.of(
                          "status",
                          TestSystemAspect.builder()
                              .urn(testEntityUrn)
                              .version(3)
                              .recordTemplate(new Status().setRemoved(false))
                              .build())));
          // fall through
        case RESTATE:
        case CREATE_ENTITY:
        case CREATE:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Incorrect Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Incorrect Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .previousSystemAspect(
                      TestSystemAspect.builder()
                          .urn(testEntityUrn)
                          .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                          .aspectSpec(
                              entityRegistry
                                  .getEntitySpec(testEntityUrn.getEntityType())
                                  .getAspectSpec("status"))
                          // Previous / actual
                          .systemMetadata(new SystemMetadata().setVersion("3"))
                          .build())
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      switch (changeType) {
        case RESTATE:
          assertEquals(Set.of(), exceptions, "Expected no exception for change type RESTATE");
          break;
        case CREATE:
        case CREATE_ENTITY:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Expected version 2, actual version -1");
          break;
        default:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Expected version 2, actual version 3",
              "for changeType:" + changeType);
          break;
      }
    }
  }

  @Test
  public void testNoSystemMetadataNextVersionNextVersionFail() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .previousSystemAspect(
                      TestSystemAspect.builder()
                          .urn(testEntityUrn)
                          .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                          .aspectSpec(
                              entityRegistry
                                  .getEntitySpec(testEntityUrn.getEntityType())
                                  .getAspectSpec("status"))
                          // Missing previous system metadata, expect fallback to version
                          .version(3)
                          .build())
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      switch (changeType) {
        case RESTATE:
          assertEquals(Set.of(), exceptions, "Expected no exception for change type RESTATE");
          break;
        case CREATE:
        case CREATE_ENTITY:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Expected version 2, actual version -1");
          break;
        default:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Expected version 2, actual version 3");
          break;
      }
    }
  }

  @Test
  public void testNoPreviousVersionsLookupSchemaMetadataNextVersionFail() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    // Prepare mock lookup based on version
    when(mockRetrieverContext
            .getAspectRetriever()
            .getLatestSystemAspects(eq(Map.of(testEntityUrn, Set.of("status")))))
        .thenReturn(
            Map.of(
                testEntityUrn,
                Map.of(
                    "status",
                    TestSystemAspect.builder()
                        .systemMetadata(
                            new SystemMetadata().setVersion("3")) // expected next version 3
                        .build())));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected is always 1
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      switch (changeType) {
        case RESTATE:
          assertEquals(Set.of(), exceptions, "Expected no exception for change type RESTATE");
          break;
        case CREATE:
        case CREATE_ENTITY:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Expected version 2, actual version -1");
          break;
        default:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Expected version 2, actual version 3");
          break;
      }
    }
  }

  @Test
  public void testNoPreviousVersionsLookupVersionNextVersionFail() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    // Prepare mock lookup based on version
    when(mockRetrieverContext
            .getAspectRetriever()
            .getLatestSystemAspects(eq(Map.of(testEntityUrn, Set.of("status")))))
        .thenReturn(
            Map.of(
                testEntityUrn,
                Map.of(
                    "status",
                    TestSystemAspect.builder()
                        .version(3) // expected next version 3
                        .build())));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected is always 1
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .headers(Map.of(HTTP_HEADER_IF_VERSION_MATCH, "2"))
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      switch (changeType) {
        case RESTATE:
          assertEquals(Set.of(), exceptions, "Expected no exception for change type RESTATE");
          break;
        case CREATE:
        case CREATE_ENTITY:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Expected version 2, actual version -1");
          break;
        default:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Expected version 2, actual version 3");
          break;
      }
    }
  }

  @Test
  public void testModifiedSinceSuccess() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    StringMap headers =
        new StringMap(
            Map.of(
                HttpHeaders.IF_UNMODIFIED_SINCE, "2024-07-03T00:00:00Z",
                HttpHeaders.IF_MODIFIED_SINCE, "2024-07-01T00:00:00Z"));
    Timestamp modified = Timestamp.from(Instant.parse("2024-07-02T00:00:00Z"));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case RESTATE:
        case CREATE_ENTITY:
        case CREATE:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(
                      new MetadataChangeProposal()
                          .setHeaders(
                              new StringMap(
                                  Map.of(
                                      HttpHeaders.IF_UNMODIFIED_SINCE,
                                      headers.get(HttpHeaders.IF_UNMODIFIED_SINCE)))))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(new MetadataChangeProposal().setHeaders(headers))
                  .previousSystemAspect(
                      TestSystemAspect.builder()
                          .urn(testEntityUrn)
                          .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                          .aspectSpec(
                              entityRegistry
                                  .getEntitySpec(testEntityUrn.getEntityType())
                                  .getAspectSpec("status"))
                          // Previous / actual
                          .auditStamp(
                              new AuditStamp()
                                  .setTime(modified.getTime())
                                  .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
                          .build())
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      assertEquals(Set.of(), exceptions, "Expected no exceptions for change type " + changeType);
    }
  }

  @Test
  public void testNoPreviousLookupAuditStampModifiedSinceSuccess() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    StringMap headers =
        new StringMap(
            Map.of(
                HttpHeaders.IF_UNMODIFIED_SINCE, "2024-07-03T00:00:00Z",
                HttpHeaders.IF_MODIFIED_SINCE, "2024-07-01T00:00:00Z"));
    Timestamp modified = Timestamp.from(Instant.parse("2024-07-02T00:00:00Z"));

    // Prepare mock lookup
    when(mockRetrieverContext
            .getAspectRetriever()
            .getLatestSystemAspects(eq(Map.of(testEntityUrn, Set.of("status")))))
        .thenReturn(
            Map.of(
                testEntityUrn,
                Map.of(
                    "status",
                    TestSystemAspect.builder()
                        .auditStamp(
                            new AuditStamp()
                                .setTime(modified.getTime())
                                .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
                        .build())));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  .metadataChangeProposal(
                      new MetadataChangeProposal()
                          .setHeaders(
                              new StringMap(
                                  Map.of(
                                      HttpHeaders.IF_UNMODIFIED_SINCE,
                                      headers.get(HttpHeaders.IF_UNMODIFIED_SINCE)))))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(new MetadataChangeProposal().setHeaders(headers))
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      assertEquals(Set.of(), exceptions, "Expected no exceptions for change type " + changeType);
    }
  }

  @Test
  public void testModifiedSinceBeforeRangeFail() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    StringMap headers =
        new StringMap(
            Map.of(
                HttpHeaders.IF_UNMODIFIED_SINCE, "2024-07-03T00:00:00Z",
                HttpHeaders.IF_MODIFIED_SINCE, "2024-07-01T00:00:00Z"));
    Timestamp modified = Timestamp.from(Instant.parse("2024-06-30T00:00:00Z"));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case RESTATE:
        case CREATE_ENTITY:
        case CREATE:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(
                      new MetadataChangeProposal()
                          .setHeaders(
                              new StringMap(
                                  Map.of(
                                      HttpHeaders.IF_UNMODIFIED_SINCE,
                                      headers.get(HttpHeaders.IF_UNMODIFIED_SINCE)))))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(new MetadataChangeProposal().setHeaders(headers))
                  .previousSystemAspect(
                      TestSystemAspect.builder()
                          .urn(testEntityUrn)
                          .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                          .aspectSpec(
                              entityRegistry
                                  .getEntitySpec(testEntityUrn.getEntityType())
                                  .getAspectSpec("status"))
                          // Previous / actual
                          .auditStamp(
                              new AuditStamp()
                                  .setTime(modified.getTime())
                                  .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
                          .build())
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
        case RESTATE:
          assertEquals(Set.of(), exceptions, "Expected no exception for change type " + changeType);
          break;
        default:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Item last modified 1719705600000 <= 1719792000000 (epoch ms)");
          break;
      }
    }
  }

  @Test
  public void testModifiedSinceAfterRangeFail() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    StringMap headers =
        new StringMap(
            Map.of(
                HttpHeaders.IF_UNMODIFIED_SINCE, "2024-07-03T00:00:00Z",
                HttpHeaders.IF_MODIFIED_SINCE, "2024-07-01T00:00:00Z"));
    Timestamp modified = Timestamp.from(Instant.parse("2024-07-04T00:00:00Z"));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case RESTATE:
        case CREATE_ENTITY:
        case CREATE:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(
                      new MetadataChangeProposal()
                          .setHeaders(
                              new StringMap(
                                  Map.of(
                                      HttpHeaders.IF_UNMODIFIED_SINCE,
                                      headers.get(HttpHeaders.IF_UNMODIFIED_SINCE)))))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(new MetadataChangeProposal().setHeaders(headers))
                  .previousSystemAspect(
                      TestSystemAspect.builder()
                          .urn(testEntityUrn)
                          .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                          .aspectSpec(
                              entityRegistry
                                  .getEntitySpec(testEntityUrn.getEntityType())
                                  .getAspectSpec("status"))
                          // Previous / actual
                          .auditStamp(
                              new AuditStamp()
                                  .setTime(modified.getTime())
                                  .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
                          .build())
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
        case RESTATE:
          assertEquals(Set.of(), exceptions, "Expected no exception for change type " + changeType);
          break;
        default:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Item last modified 1720051200000 > 1719964800000 (epoch ms)");
          break;
      }
    }
  }

  @Test
  public void testNoPreviousLookupAuditStampModifiedSinceBeforeRangeFail() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    StringMap headers =
        new StringMap(
            Map.of(
                HttpHeaders.IF_UNMODIFIED_SINCE, "2024-07-03T00:00:00Z",
                HttpHeaders.IF_MODIFIED_SINCE, "2024-07-01T00:00:00Z"));
    Timestamp modified = Timestamp.from(Instant.parse("2024-06-30T00:00:00Z"));

    // Prepare mock lookup
    when(mockRetrieverContext
            .getAspectRetriever()
            .getLatestSystemAspects(eq(Map.of(testEntityUrn, Set.of("status")))))
        .thenReturn(
            Map.of(
                testEntityUrn,
                Map.of(
                    "status",
                    TestSystemAspect.builder()
                        .auditStamp(
                            new AuditStamp()
                                .setTime(modified.getTime())
                                .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
                        .build())));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  .metadataChangeProposal(
                      new MetadataChangeProposal()
                          .setHeaders(
                              new StringMap(
                                  Map.of(
                                      HttpHeaders.IF_UNMODIFIED_SINCE,
                                      headers.get(HttpHeaders.IF_UNMODIFIED_SINCE)))))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(new MetadataChangeProposal().setHeaders(headers))
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
        case RESTATE:
          assertEquals(Set.of(), exceptions, "Expected no exception for change type " + changeType);
          break;
        default:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Item last modified 1719705600000 <= 1719792000000 (epoch ms)");
          break;
      }
    }
  }

  @Test
  public void testNoPreviousLookupAuditStampModifiedSinceAfterRangeFail() {
    ConditionalWriteValidator test = new ConditionalWriteValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    StringMap headers =
        new StringMap(
            Map.of(
                HttpHeaders.IF_UNMODIFIED_SINCE, "2024-07-03T00:00:00Z",
                HttpHeaders.IF_MODIFIED_SINCE, "2024-07-01T00:00:00Z"));
    Timestamp modified = Timestamp.from(Instant.parse("2024-07-04T00:00:00Z"));

    // Prepare mock lookup
    when(mockRetrieverContext
            .getAspectRetriever()
            .getLatestSystemAspects(eq(Map.of(testEntityUrn, Set.of("status")))))
        .thenReturn(
            Map.of(
                testEntityUrn,
                Map.of(
                    "status",
                    TestSystemAspect.builder()
                        .auditStamp(
                            new AuditStamp()
                                .setTime(modified.getTime())
                                .setActor(UrnUtils.getUrn("urn:li:corpuser:test")))
                        .build())));

    for (ChangeType changeType : supportedChangeTypes) {
      final ChangeMCP testMCP;
      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  .metadataChangeProposal(
                      new MetadataChangeProposal()
                          .setHeaders(
                              new StringMap(
                                  Map.of(
                                      HttpHeaders.IF_UNMODIFIED_SINCE,
                                      headers.get(HttpHeaders.IF_UNMODIFIED_SINCE)))))
                  .build();
          break;
        default:
          testMCP =
              TestMCP.builder()
                  .changeType(changeType)
                  .urn(testEntityUrn)
                  .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                  .aspectSpec(
                      entityRegistry
                          .getEntitySpec(testEntityUrn.getEntityType())
                          .getAspectSpec("status"))
                  .recordTemplate(new Status().setRemoved(false))
                  // Expected
                  .metadataChangeProposal(new MetadataChangeProposal().setHeaders(headers))
                  .build();
          break;
      }

      Set<AspectValidationException> exceptions =
          test.validatePreCommit(List.of(testMCP), mockRetrieverContext)
              .collect(Collectors.toSet());

      switch (changeType) {
        case CREATE:
        case CREATE_ENTITY:
        case RESTATE:
          assertEquals(Set.of(), exceptions, "Expected no exception for change type " + changeType);
          break;
        default:
          assertEquals(exceptions.size(), 1, "Expected exception for change type " + changeType);
          assertEquals(
              exceptions.stream().findFirst().get().getMessage(),
              "Item last modified 1720051200000 > 1719964800000 (epoch ms)");
          break;
      }
    }
  }
}
