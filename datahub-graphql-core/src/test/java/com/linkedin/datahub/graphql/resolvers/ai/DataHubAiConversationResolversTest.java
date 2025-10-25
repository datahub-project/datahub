package com.linkedin.datahub.graphql.resolvers.ai;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.service.DataHubAiConversationService;
import graphql.schema.idl.RuntimeWiring;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubAiConversationResolversTest {

  private DataHubAiConversationService mockConversationService;
  private DataHubAiConversationResolvers resolvers;

  @BeforeMethod
  public void setUp() {
    mockConversationService = mock(DataHubAiConversationService.class);
    resolvers = new DataHubAiConversationResolvers(mockConversationService);
  }

  @Test
  public void testConstructor() {
    // Test that constructor properly sets the conversation service
    assertNotNull(resolvers);
    // We can't directly access the private field, but we can verify the service is used
    // in the configureResolvers method
  }

  @Test
  public void testConfigureResolvers() {
    // Create a mock RuntimeWiring.Builder
    RuntimeWiring.Builder mockBuilder = mock(RuntimeWiring.Builder.class);

    // Setup the mock to return itself for chaining
    when(mockBuilder.type(anyString(), any())).thenReturn(mockBuilder);

    // Execute the method
    resolvers.configureResolvers(mockBuilder);

    // Verify that both Query and Mutation types are configured
    verify(mockBuilder, times(1)).type(eq("Query"), any());
    verify(mockBuilder, times(1)).type(eq("Mutation"), any());
  }

  @Test
  public void testConfigureResolversWithNullBuilder() {
    // Test that the method handles null builder gracefully
    try {
      resolvers.configureResolvers(null);
      // If no exception is thrown, the method should handle null gracefully
    } catch (Exception e) {
      // If an exception is thrown, it should be a NullPointerException
      assertTrue(e instanceof NullPointerException);
    }
  }

  @Test
  public void testConfigureResolversWithMockBuilder() {
    // Test with a more realistic mock setup
    RuntimeWiring.Builder mockBuilder = mock(RuntimeWiring.Builder.class);

    // Setup the mock to return itself for chaining
    when(mockBuilder.type(anyString(), any())).thenReturn(mockBuilder);

    // Execute the method
    resolvers.configureResolvers(mockBuilder);

    // Verify that the builder was used to configure resolvers
    verify(mockBuilder, atLeastOnce()).type(anyString(), any());
  }
}
