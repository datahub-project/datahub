package io.datahubproject.metadata.context;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OperationContextTest {
  private TraceContext mockTraceContext;
  private SystemMetadata mockSystemMetadata;
  private ObjectMapper mockObjectMapper;

  @BeforeMethod
  public void setUp() {
    mockTraceContext = mock(TraceContext.class);
    mockSystemMetadata = mock(SystemMetadata.class);
    mockObjectMapper = mock(ObjectMapper.class);
  }

  @Test
  public void testSystemPrivilegeEscalation() {
    Authentication systemAuth = new Authentication(new Actor(ActorType.USER, "SYSTEM"), "");
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "USER"), "");

    // Allows system authentication
    OperationContext systemOpContext =
        OperationContext.asSystem(
            OperationContextConfig.builder().build(),
            systemAuth,
            mock(EntityRegistry.class),
            mock(ServicesRegistryContext.class),
            null,
            TestOperationContexts.emptyActiveUsersRetrieverContext(null),
            mock(ValidationContext.class),
            null,
            true);

    OperationContext opContext =
        systemOpContext.asSession(RequestContext.TEST, Authorizer.EMPTY, userAuth);

    assertEquals(
        opContext.getAuthentication(), systemAuth, "Expected system authentication when allowed");
    assertEquals(
        opContext.getAuditStamp().getActor().getId(),
        "USER",
        "Audit stamp expected to match the user's identity");
    assertEquals(opContext.getSessionAuthentication(), userAuth);
    assertEquals(opContext.getSessionActorContext().getAuthentication(), userAuth);
    assertEquals(opContext.getActorContext().getAuthentication(), systemAuth);
    assertEquals(opContext.getSystemActorContext().getAuthentication(), systemAuth);
    assertEquals(opContext.getSystemAuthentication().get(), systemAuth);

    // Do not allow system auth
    OperationContext opContextNoSystem =
        systemOpContext.toBuilder()
            .operationContextConfig(
                systemOpContext.getOperationContextConfig().toBuilder()
                    .allowSystemAuthentication(false)
                    .build())
            .build(userAuth, true);

    assertEquals(
        opContextNoSystem.getAuthentication(),
        userAuth,
        "Expect user authentication when system authentication is not allowed");
    assertEquals(
        opContextNoSystem.getAuditStamp().getActor().getId(),
        "USER",
        "Audit stamp expected to match the user's identity");
    assertEquals(opContextNoSystem.getSessionActorContext().getAuthentication(), userAuth);
    assertEquals(opContextNoSystem.getActorContext().getAuthentication(), userAuth);
    assertEquals(opContextNoSystem.getSystemActorContext().getAuthentication(), systemAuth);
    assertEquals(opContextNoSystem.getSystemAuthentication().get(), systemAuth);
    assertEquals(opContextNoSystem.getSystemActorContext().getAuthentication(), systemAuth);
    assertEquals(opContextNoSystem.getSessionAuthentication(), userAuth);
  }

  @Test
  public void testWithTraceId_WithTraceContextAndSystemMetadata() {
    when(mockTraceContext.withTraceId(eq(mockSystemMetadata), anyBoolean()))
        .thenReturn(mockSystemMetadata);

    SystemMetadata result = buildTraceMock().withTraceId(mockSystemMetadata);

    verify(mockTraceContext).withTraceId(mockSystemMetadata, false);
    assertEquals(result, mockSystemMetadata);
  }

  @Test
  public void testWithTraceId_NullSystemMetadata() {
    SystemMetadata result = buildTraceMock().withTraceId(null);

    verifyNoInteractions(mockTraceContext);
    assertNull(result);
  }

  @Test
  public void testWithTraceId_NullTraceContext() {
    OperationContext operationContext = buildTraceMock(() -> null);

    SystemMetadata result = operationContext.withTraceId(mockSystemMetadata);

    assertEquals(result, mockSystemMetadata);
  }

  @Test
  public void testWithSpan_WithTraceContext() {
    String spanName = "testSpan";
    String[] attributes = {"attr1", "attr2"};
    final boolean[] operationExecuted = {false};
    Supplier<String> operation =
        () -> {
          operationExecuted[0] = true;
          return "result";
        };

    // Capture the supplier passed to withSpan to verify it's executed
    doAnswer(
            invocation -> {
              Supplier<?> capturedSupplier = invocation.getArgument(1);
              return capturedSupplier.get();
            })
        .when(mockTraceContext)
        .withSpan(eq(spanName), any(Supplier.class), eq(attributes));

    String result = buildTraceMock().withSpan(spanName, operation, attributes);

    verify(mockTraceContext).withSpan(eq(spanName), any(Supplier.class), eq(attributes));
    assertTrue(operationExecuted[0], "The operation supplier should have been executed");
    assertEquals(result, "result", "The result should match the operation's return value");
  }

  @Test
  public void testWithSpan_NullTraceContext() {
    OperationContext operationContext = buildTraceMock(() -> null);

    String spanName = "testSpan";
    String[] attributes = {"attr1", "attr2"};
    Supplier<String> operation = () -> "result";

    String result = operationContext.withSpan(spanName, operation, attributes);

    assertEquals(result, "result");
  }

  @Test
  public void testWithSpan_RunnableWithTraceContext() {
    String spanName = "testSpan";
    String[] attributes = {"attr1", "attr2"};
    Runnable operation = mock(Runnable.class);

    buildTraceMock().withSpan(spanName, operation, attributes);

    verify(mockTraceContext).withSpan(eq(spanName), eq(operation), eq(attributes));
    verifyNoMoreInteractions(operation);
  }

  @Test
  public void testWithQueueSpan_SingleSystemMetadata() {
    String spanName = "testQueueSpan";
    String topicName = "testTopic";
    String[] attributes = {"attr1", "attr2"};
    Runnable operation = mock(Runnable.class);

    buildTraceMock().withQueueSpan(spanName, mockSystemMetadata, topicName, operation, attributes);

    verify(mockTraceContext)
        .withQueueSpan(
            eq(spanName),
            eq(List.of(mockSystemMetadata)),
            eq(topicName),
            eq(operation),
            eq(attributes));
  }

  @Test
  public void testWithQueueSpan_MultipleSystemMetadata() {
    String spanName = "testQueueSpan";
    String topicName = "testTopic";
    String[] attributes = {"attr1", "attr2"};
    Runnable operation = mock(Runnable.class);
    List<SystemMetadata> systemMetadataList = Arrays.asList(mockSystemMetadata, mockSystemMetadata);

    buildTraceMock().withQueueSpan(spanName, systemMetadataList, topicName, operation, attributes);

    verify(mockTraceContext)
        .withQueueSpan(
            eq(spanName), eq(systemMetadataList), eq(topicName), eq(operation), eq(attributes));
  }

  @Test
  public void testTraceException() throws Exception {
    Set<Throwable> throwables = new HashSet<>();
    throwables.add(new RuntimeException("test exception 1"));
    throwables.add(new IllegalArgumentException("test exception 2"));

    String expectedJson = "[{\"message\":\"test exception 1\"},{\"message\":\"test exception 2\"}]";
    when(mockObjectMapper.writeValueAsString(any())).thenReturn(expectedJson);

    String result = buildTraceMock().traceException(throwables);

    verify(mockObjectMapper).writeValueAsString(any());
    assertEquals(result, expectedJson);
  }

  @Test
  public void testTraceException_JsonProcessingError() throws Exception {
    Set<Throwable> throwables = new HashSet<>();
    RuntimeException ex1 = new RuntimeException("test exception 1");
    IllegalArgumentException ex2 = new IllegalArgumentException("test exception 2");
    throwables.add(ex1);
    throwables.add(ex2);

    when(mockObjectMapper.writeValueAsString(any()))
        .thenThrow(new com.fasterxml.jackson.core.JsonProcessingException("") {});

    String result = buildTraceMock().traceException(throwables);

    verify(mockObjectMapper).writeValueAsString(any());
    assertTrue(result.contains("test exception 1"));
    assertTrue(result.contains("test exception 2"));
  }

  private OperationContext buildTraceMock() {
    return buildTraceMock(null);
  }

  private OperationContext buildTraceMock(Supplier<TraceContext> traceContextSupplier) {
    return TestOperationContexts.systemContextTraceNoSearchAuthorization(
        () -> ObjectMapperContext.builder().objectMapper(mockObjectMapper).build(),
        traceContextSupplier == null ? () -> mockTraceContext : traceContextSupplier);
  }
}
