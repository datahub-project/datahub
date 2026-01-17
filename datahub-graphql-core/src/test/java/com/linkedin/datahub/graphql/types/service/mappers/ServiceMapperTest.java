package com.linkedin.datahub.graphql.types.service.mappers;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.McpTransport;
import com.linkedin.datahub.graphql.generated.Service;
import com.linkedin.datahub.graphql.generated.ServiceSubType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.service.McpServerProperties;
import com.linkedin.service.ServiceProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ServiceMapperTest {

  private QueryContext queryContext;

  @BeforeMethod
  public void setup() {
    queryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapServiceWithAllAspects() {
    // Setup
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:test-mcp-server");

    // Create ServiceProperties aspect
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("Test MCP Server");
    serviceProperties.setDescription("A test MCP server for unit testing");

    // Create SubTypes aspect
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("MCP_SERVER"));

    // Create McpServerProperties aspect
    McpServerProperties mcpProperties = new McpServerProperties();
    mcpProperties.setUrl("https://mcp.example.com/api");
    mcpProperties.setTransport(com.linkedin.service.McpTransport.HTTP);
    mcpProperties.setTimeout(60.0f);
    StringMap customHeaders = new StringMap();
    customHeaders.put("X-Custom-Header", "custom-value");
    mcpProperties.setCustomHeaders(customHeaders);

    // Create Status aspect
    Status status = new Status();
    status.setRemoved(false);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));
    aspects.put(
        Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(mcpProperties.data())));
    aspects.put(
        Constants.STATUS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(status.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getUrn(), serviceUrn.toString());

    // Verify properties
    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getDisplayName(), "Test MCP Server");
    assertEquals(result.getProperties().getDescription(), "A test MCP server for unit testing");

    // Verify subType from subTypes aspect
    assertEquals(result.getSubType(), ServiceSubType.MCP_SERVER);

    // Verify MCP properties
    assertNotNull(result.getMcpServerProperties());
    assertEquals(result.getMcpServerProperties().getUrl(), "https://mcp.example.com/api");
    assertEquals(result.getMcpServerProperties().getTransport(), McpTransport.HTTP);
    assertEquals(result.getMcpServerProperties().getTimeout(), 60.0);
    assertNotNull(result.getMcpServerProperties().getCustomHeaders());
    assertEquals(result.getMcpServerProperties().getCustomHeaders().size(), 1);
    assertEquals(
        result.getMcpServerProperties().getCustomHeaders().get(0).getKey(), "X-Custom-Header");
    assertEquals(
        result.getMcpServerProperties().getCustomHeaders().get(0).getValue(), "custom-value");

    // Verify status
    assertNotNull(result.getStatus());
    assertFalse(result.getStatus().getRemoved());
  }

  @Test
  public void testMapServiceWithMinimalAspects() {
    // Setup - only required aspects
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:minimal-service");

    // Create minimal ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("Minimal Service");

    // Build EntityResponse with only service properties
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getUrn(), serviceUrn.toString());
    assertEquals(result.getProperties().getDisplayName(), "Minimal Service");
    assertNull(result.getProperties().getDescription());
    assertNull(result.getSubType()); // No subTypes aspect
    assertNull(result.getMcpServerProperties()); // No MCP properties
    assertNull(result.getStatus()); // No status
  }

  @Test
  public void testMapServiceWithSseTransport() {
    // Setup
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:sse-server");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("SSE MCP Server");

    // Create SubTypes aspect
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("MCP_SERVER"));

    // Create McpServerProperties with SSE transport
    McpServerProperties mcpProperties = new McpServerProperties();
    mcpProperties.setUrl("https://sse.example.com/events");
    mcpProperties.setTransport(com.linkedin.service.McpTransport.SSE);
    mcpProperties.setTimeout(120.0f);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));
    aspects.put(
        Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(mcpProperties.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getMcpServerProperties().getTransport(), McpTransport.SSE);
    assertEquals(result.getMcpServerProperties().getTimeout(), 120.0);
  }

  @Test
  public void testMapServiceWithWebsocketTransport() {
    // Setup
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:websocket-server");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("WebSocket MCP Server");

    // Create SubTypes aspect
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("MCP_SERVER"));

    // Create McpServerProperties with WebSocket transport
    McpServerProperties mcpProperties = new McpServerProperties();
    mcpProperties.setUrl("wss://ws.example.com/mcp");
    mcpProperties.setTransport(com.linkedin.service.McpTransport.WEBSOCKET);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));
    aspects.put(
        Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(mcpProperties.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getMcpServerProperties().getTransport(), McpTransport.WEBSOCKET);
    assertEquals(result.getMcpServerProperties().getUrl(), "wss://ws.example.com/mcp");
  }

  @Test
  public void testMapServiceWithRemovedStatus() {
    // Setup - service that has been soft-deleted
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:deleted-service");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("Deleted Service");

    // Create Status with removed=true
    Status status = new Status();
    status.setRemoved(true);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.STATUS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(status.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertNotNull(result.getStatus());
    assertTrue(result.getStatus().getRemoved());
  }

  @Test
  public void testMapServiceWithMultipleSubTypes() {
    // Setup - service with multiple subtypes (takes first one)
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:multi-subtype");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("Multi-Subtype Service");

    // Create SubTypes with multiple values
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("MCP_SERVER", "OTHER_TYPE"));

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify - should use first subType
    assertNotNull(result);
    assertEquals(result.getSubType(), ServiceSubType.MCP_SERVER);
  }

  @Test
  public void testMapServiceWithEmptySubTypes() {
    // Setup - service with empty subtypes array
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:empty-subtype");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("Empty Subtype Service");

    // Create SubTypes with empty array
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray());

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify - should have null subType when array is empty
    assertNotNull(result);
    assertNull(result.getSubType());
  }

  @Test
  public void testMapServiceWithUnknownSubType() {
    // Setup - service with unknown subtype (not in enum)
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:unknown-subtype");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("Unknown Subtype Service");

    // Create SubTypes with unknown value
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("UNKNOWN_TYPE"));

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute - mapper throws IllegalArgumentException for unknown subtype
    assertThrows(
        IllegalArgumentException.class, () -> ServiceMapper.map(queryContext, entityResponse));
  }

  @Test
  public void testMapServiceWithMultipleCustomHeaders() {
    // Setup
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:multi-header");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("Multi-Header Service");

    // Create SubTypes aspect
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("MCP_SERVER"));

    // Create McpServerProperties with multiple headers
    McpServerProperties mcpProperties = new McpServerProperties();
    mcpProperties.setUrl("https://api.example.com");
    mcpProperties.setTransport(com.linkedin.service.McpTransport.HTTP);
    StringMap customHeaders = new StringMap();
    customHeaders.put("X-Api-Key", "key123");
    customHeaders.put("X-Tenant-Id", "tenant456");
    customHeaders.put("Accept", "application/json");
    mcpProperties.setCustomHeaders(customHeaders);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));
    aspects.put(
        Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(mcpProperties.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertNotNull(result.getMcpServerProperties().getCustomHeaders());
    assertEquals(result.getMcpServerProperties().getCustomHeaders().size(), 3);
  }

  @Test
  public void testMapServiceWithNoCustomHeaders() {
    // Setup
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:no-headers");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("No Headers Service");

    // Create SubTypes aspect
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("MCP_SERVER"));

    // Create McpServerProperties without headers
    McpServerProperties mcpProperties = new McpServerProperties();
    mcpProperties.setUrl("https://simple.example.com");
    mcpProperties.setTransport(com.linkedin.service.McpTransport.HTTP);
    // No custom headers set

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));
    aspects.put(
        Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(mcpProperties.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertNotNull(result.getMcpServerProperties());
    assertNull(result.getMcpServerProperties().getCustomHeaders());
  }

  @Test
  public void testMapServiceWithEmptyCustomHeaders() {
    // Setup
    Urn serviceUrn = UrnUtils.getUrn("urn:li:service:empty-headers");

    // Create ServiceProperties
    ServiceProperties serviceProperties = new ServiceProperties();
    serviceProperties.setDisplayName("Empty Headers Service");

    // Create SubTypes aspect
    SubTypes subTypes = new SubTypes();
    subTypes.setTypeNames(new StringArray("MCP_SERVER"));

    // Create McpServerProperties with empty headers map
    McpServerProperties mcpProperties = new McpServerProperties();
    mcpProperties.setUrl("https://empty.example.com");
    mcpProperties.setTransport(com.linkedin.service.McpTransport.HTTP);
    mcpProperties.setCustomHeaders(new StringMap());

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(serviceProperties.data())));
    aspects.put(
        Constants.SUB_TYPES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(subTypes.data())));
    aspects.put(
        Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(mcpProperties.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serviceUrn);
    entityResponse.setEntityName(Constants.SERVICE_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    Service result = ServiceMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertNotNull(result.getMcpServerProperties());
    // Empty map should result in empty list
    assertNotNull(result.getMcpServerProperties().getCustomHeaders());
    assertTrue(result.getMcpServerProperties().getCustomHeaders().isEmpty());
  }
}
