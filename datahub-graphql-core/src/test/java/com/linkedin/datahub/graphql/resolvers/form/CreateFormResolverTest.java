package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateFormInput;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.FormType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateFormResolverTest {
  private static final String TEST_FORM_URN = "urn:li:form:1";

  private static final CreateFormInput TEST_INPUT =
      new CreateFormInput(null, "test name", null, FormType.VERIFICATION, new ArrayList<>(), null);

  @Test
  public void testGetSuccess() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockEntityClient = initMockEntityClient();
    CreateFormResolver resolver = new CreateFormResolver(mockEntityClient, mockFormService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Form form = resolver.get(mockEnv).get();

    assertEquals(form.getUrn(), TEST_FORM_URN);

    // Validate that we called create on the service
    Mockito.verify(mockFormService, Mockito.times(1))
        .createForm(any(), any(FormInfo.class), Mockito.eq(null));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    FormService mockFormService = initMockFormService(true);
    EntityClient mockEntityClient = initMockEntityClient();
    CreateFormResolver resolver = new CreateFormResolver(mockEntityClient, mockFormService);

    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we did NOT call create on the service
    Mockito.verify(mockFormService, Mockito.times(0))
        .createForm(any(), any(FormInfo.class), Mockito.eq(null));
  }

  @Test
  public void testGetFailure() throws Exception {
    FormService mockFormService = initMockFormService(false);
    EntityClient mockEntityClient = initMockEntityClient();
    CreateFormResolver resolver = new CreateFormResolver(mockEntityClient, mockFormService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Validate that we called create on the service
    Mockito.verify(mockFormService, Mockito.times(1))
        .createForm(any(), any(FormInfo.class), Mockito.eq(null));
  }

  private FormService initMockFormService(final boolean shouldSucceed) throws Exception {
    FormService service = Mockito.mock(FormService.class);

    if (shouldSucceed) {
      Mockito.when(service.createForm(any(), Mockito.any(), Mockito.any()))
          .thenReturn(UrnUtils.getUrn("urn:li:form:1"));
    } else {
      Mockito.when(service.createForm(any(), Mockito.any(), Mockito.any()))
          .thenThrow(new RuntimeException());
    }

    return service;
  }

  private EntityClient initMockEntityClient() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    EntityResponse response = new EntityResponse();
    response.setEntityName(Constants.FORM_ENTITY_NAME);
    response.setUrn(UrnUtils.getUrn(TEST_FORM_URN));
    response.setAspects(new EnvelopedAspectMap());
    Mockito.when(
            client.getV2(any(), Mockito.eq(Constants.FORM_ENTITY_NAME), any(), Mockito.eq(null)))
        .thenReturn(response);

    return client;
  }
}
