package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateApplicationInput;
import com.linkedin.datahub.graphql.generated.CreateApplicationPropertiesInput;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ApplicationService;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateApplicationResolverTest {

  private static final String TEST_APP_ID = "test-app-id";
  private static final String TEST_APP_NAME = "Test Application";
  private static final String TEST_APP_DESCRIPTION = "This is a test application.";
  private static final Urn TEST_APP_URN = Urn.createFromTuple(APPLICATION_ENTITY_NAME, TEST_APP_ID);
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  private ApplicationService mockApplicationService;
  private EntityService
      mockEntityService; // Keep for verifyEntityExists if CreateApplicationResolver uses it
  private CreateApplicationResolver resolver;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setupTest() {
    mockApplicationService = Mockito.mock(ApplicationService.class);
    mockEntityService = Mockito.mock(EntityService.class); // if CreateApplicationResolver needs it.
    resolver = new CreateApplicationResolver(mockApplicationService, mockEntityService);
    mockContext = getMockAllowContext(TEST_ACTOR_URN.toString());
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testGetSuccess() throws Exception {
    CreateApplicationPropertiesInput propertiesInput =
        CreateApplicationPropertiesInput.builder()
            .setName(TEST_APP_NAME)
            .setDescription(TEST_APP_DESCRIPTION)
            .build();
    CreateApplicationInput input =
        CreateApplicationInput.builder().setId(TEST_APP_ID).setProperties(propertiesInput).build();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    // Mock the ApplicationService call
    Mockito.when(
            mockApplicationService.createApplication(
                any(), eq(TEST_APP_ID), eq(TEST_APP_NAME), eq(TEST_APP_DESCRIPTION)))
        .thenReturn(TEST_APP_URN);

    Mockito.when(mockApplicationService.verifyEntityExists(any(), eq(TEST_APP_URN)))
        .thenReturn(false);
    Mockito.when(mockApplicationService.getApplicationEntityResponse(any(), eq(TEST_APP_URN)))
        .thenReturn(null);

    resolver.get(mockEnv).get();

    Mockito.verify(mockApplicationService, Mockito.times(1))
        .createApplication(any(), eq(TEST_APP_ID), eq(TEST_APP_NAME), eq(TEST_APP_DESCRIPTION));
  }

  @Test
  public void testGetSuccessIdGenerated() throws Exception {
    CreateApplicationPropertiesInput propertiesInput =
        CreateApplicationPropertiesInput.builder()
            .setName(TEST_APP_NAME)
            .setDescription(TEST_APP_DESCRIPTION)
            .build();
    CreateApplicationInput input =
        CreateApplicationInput.builder()
            .setProperties(propertiesInput)
            .build(); // ID is null, so it should be generated
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    String generatedId = UUID.randomUUID().toString();
    Urn generatedAppUrn = Urn.createFromTuple(APPLICATION_ENTITY_NAME, generatedId);

    Mockito.when(
            mockApplicationService.createApplication(
                any(), eq(null), eq(TEST_APP_NAME), eq(TEST_APP_DESCRIPTION)))
        .thenReturn(generatedAppUrn);
    Mockito.when(mockApplicationService.verifyEntityExists(any(), eq(generatedAppUrn)))
        .thenReturn(false);
    Mockito.when(mockApplicationService.getApplicationEntityResponse(any(), eq(generatedAppUrn)))
        .thenReturn(null);

    resolver.get(mockEnv).get();

    Mockito.verify(mockApplicationService, Mockito.times(1))
        .createApplication(any(), eq(null), eq(TEST_APP_NAME), eq(TEST_APP_DESCRIPTION));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    CreateApplicationPropertiesInput propertiesInput =
        CreateApplicationPropertiesInput.builder()
            .setName(TEST_APP_NAME)
            .setDescription(TEST_APP_DESCRIPTION)
            .build();
    CreateApplicationInput input =
        CreateApplicationInput.builder().setId(TEST_APP_ID).setProperties(propertiesInput).build();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockDenyContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockDenyContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockApplicationService, Mockito.never())
        .createApplication(any(), any(), any(), any());
  }
}
