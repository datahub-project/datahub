package com.linkedin.datahub.graphql.resolvers.knowledge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.group.GroupService;
import com.linkedin.datahub.graphql.types.dataplatform.DataPlatformType;
import com.linkedin.datahub.graphql.types.dataplatforminstance.DataPlatformInstanceType;
import com.linkedin.datahub.graphql.types.knowledge.DocumentType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.idl.RuntimeWiring;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DocumentResolversTest {

  private DocumentService mockService;
  private DocumentType mockType;
  private DataPlatformType mockDataPlatformType;
  private DataPlatformInstanceType mockDataPlatformInstanceType;
  private EntityClient mockEntityClient;
  private EntityService mockEntityService;
  private GraphClient mockGraphClient;
  private EntityRegistry mockEntityRegistry;
  private com.linkedin.metadata.timeline.TimelineService mockTimelineService;
  private GroupService mockGroupService;
  private DocumentResolvers resolvers;

  @BeforeMethod
  public void setUp() {
    mockService = mock(DocumentService.class);
    mockType = mock(DocumentType.class);
    mockDataPlatformType = mock(DataPlatformType.class);
    mockDataPlatformInstanceType = mock(DataPlatformInstanceType.class);
    mockEntityClient = mock(EntityClient.class);
    mockEntityService = mock(EntityService.class);
    mockGraphClient = mock(GraphClient.class);
    mockEntityRegistry = mock(EntityRegistry.class);
    mockTimelineService = mock(com.linkedin.metadata.timeline.TimelineService.class);
    mockGroupService = mock(GroupService.class);

    resolvers =
        new DocumentResolvers(
            mockService,
            (List) java.util.Collections.emptyList(),
            mockType,
            mockDataPlatformType,
            mockDataPlatformInstanceType,
            mockEntityClient,
            mockEntityService,
            mockGraphClient,
            mockEntityRegistry,
            mockTimelineService,
            mockGroupService);
  }

  @Test
  public void testConstructor() {
    assertNotNull(resolvers);
  }

  @Test
  public void testConfigureResolvers() {
    RuntimeWiring.Builder mockBuilder = mock(RuntimeWiring.Builder.class);
    when(mockBuilder.type(anyString(), any())).thenReturn(mockBuilder);

    resolvers.configureResolvers(mockBuilder);

    // Verify Query and Mutation types were configured
    verify(mockBuilder, times(1)).type(eq("Query"), any());
    verify(mockBuilder, times(1)).type(eq("Mutation"), any());

    // Verify Document type and related info types are wired
    verify(mockBuilder, times(1)).type(eq("Document"), any());
    verify(mockBuilder, times(1)).type(eq("DocumentRelatedAsset"), any());
    verify(mockBuilder, times(1)).type(eq("DocumentRelatedDocument"), any());
    verify(mockBuilder, times(1)).type(eq("DocumentParentDocument"), any());
  }
}
