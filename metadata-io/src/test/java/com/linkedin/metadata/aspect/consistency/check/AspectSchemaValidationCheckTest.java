package com.linkedin.metadata.aspect.consistency.check;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for AspectSchemaValidationCheck. */
public class AspectSchemaValidationCheckTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private OperationContext mockOpContext;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private RetrieverContext mockRetrieverContext;
  @Mock private AspectRetriever mockAspectRetriever;

  private CheckContext checkContext;
  private AspectSchemaValidationCheck check;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(mockEntityRegistry);

    checkContext =
        CheckContext.builder()
            .operationContext(mockOpContext)
            .entityService(mockEntityService)
            .build();

    check = new AspectSchemaValidationCheck();
  }

  // ============================================================================
  // Basic Properties Tests
  // ============================================================================

  @Test
  public void testEntityTypeIsWildcard() {
    assertEquals(check.getEntityType(), ConsistencyCheckRegistry.WILDCARD_ENTITY_TYPE);
  }

  @Test
  public void testRequiresAllAspects() {
    // AspectSchemaValidationCheck returns empty Optional = needs all aspects
    assertTrue(check.getRequiredAspects().isEmpty());
  }

  @Test
  public void testIsOnDemandOnly() {
    assertTrue(check.isOnDemandOnly());
  }

  @Test
  public void testIdDerivedFromClassName() {
    assertEquals(check.getId(), "aspect-schema-validation");
  }

  // ============================================================================
  // Entity Type Resolution Tests
  // ============================================================================

  @Test
  public void testSkipsWhenUnknownEntityType() {
    Urn urn = UrnUtils.getUrn("urn:li:unknownEntity:test123");

    when(mockEntityRegistry.getEntitySpec("unknownEntity"))
        .thenThrow(new IllegalArgumentException("Unknown entity type"));

    EntityResponse response = createEntityResponse(urn, "unknownEntity");

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(urn, response));

    assertTrue(issues.isEmpty());
  }

  // ============================================================================
  // Unknown Aspect Handling Tests
  // ============================================================================

  @Test
  public void testSkipsWhenUnknownAspect() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    EntitySpec entitySpec = mock(EntitySpec.class);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(entitySpec);
    // Aspect spec not found
    when(entitySpec.getAspectSpec("unknownAspect")).thenReturn(null);

    EntityResponse response = createEntityResponseWithUnknownAspect(urn);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(urn, response));

    assertTrue(issues.isEmpty());
  }

  // ============================================================================
  // Valid Aspect Tests
  // ============================================================================

  @SuppressWarnings("unchecked")
  @Test
  public void testReturnsEmptyWhenAspectIsValid() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    EntitySpec entitySpec = mock(EntitySpec.class);
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(entitySpec);
    when(entitySpec.getAspectSpec(STATUS_ASPECT_NAME)).thenReturn(aspectSpec);
    doReturn(Status.class).when(aspectSpec).getDataTemplateClass();

    EntityResponse response = createValidEntityResponse(urn);

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(urn, response));

    // A valid Status aspect should not trigger any issues
    assertTrue(issues.isEmpty());
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private EntityResponse createEntityResponse(Urn urn, String entityName) {
    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(entityName);
    response.setAspects(new EnvelopedAspectMap());
    return response;
  }

  private EntityResponse createEntityResponseWithUnknownAspect(Urn urn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName("dataset");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect unknownAspect = new EnvelopedAspect();
    DataMap data = new DataMap();
    data.put("someField", "someValue");
    unknownAspect.setValue(new Aspect(data));
    aspects.put("unknownAspect", unknownAspect);

    response.setAspects(aspects);
    return response;
  }

  private EntityResponse createValidEntityResponse(Urn urn) {
    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName("dataset");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    EnvelopedAspect statusAspect = new EnvelopedAspect();
    Status status = new Status().setRemoved(false);
    statusAspect.setValue(new Aspect(status.data()));
    aspects.put(STATUS_ASPECT_NAME, statusAspect);

    response.setAspects(aspects);
    return response;
  }
}
