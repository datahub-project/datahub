package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContextForResource;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.form.FormPromptType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SubmitFormPromptResolverTest {

  private static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)";
  private static final String FORM_URN = "urn:li:form:1";
  private static final String PROMPT_ID = "prompt-1";
  private static final String PROPERTY_URN = "urn:li:structuredProperty:prop1";

  @Test
  public void testAssignedUserWithoutEditPrivilegesCanSubmit() throws Exception {
    FormService formService = mockFormService(true, true);

    assertTrue(submit(formService, getMockDenyContext(), "STRUCTURED_PROPERTY"));
  }

  @Test
  public void testUnassignedUserWithoutEditPrivilegesIsDeniedBeforeAnyWrite() throws Exception {
    FormService formService = mockFormService(true, false);

    assertThrows(
        CompletionException.class,
        () -> submit(formService, getMockDenyContext(), "STRUCTURED_PROPERTY"));
    assertNothingSubmitted(formService);
  }

  @Test
  public void testUnassignedUserWhoCanEditPropertiesCanSubmit() throws Exception {
    FormService formService = mockFormService(true, false);

    assertTrue(submit(formService, getMockAllowContext(), "STRUCTURED_PROPERTY"));
  }

  @Test
  public void testResponseForAnotherPropertyIsDeniedBeforeAnyWrite() throws Exception {
    FormService formService = mockFormService(false, true);

    assertThrows(
        CompletionException.class,
        () -> submit(formService, getMockAllowContext(), "STRUCTURED_PROPERTY"));
    assertNothingSubmitted(formService);
  }

  @Test
  public void testFieldPromptAllowsColumnPropertiesAndDeniesEntityProperties() throws Exception {
    FormService allowed = mockFormService(true, false);
    assertTrue(
        submit(
            allowed,
            privilegeOnDataset(PoliciesConfig.EDIT_DATASET_COL_PROPERTIES_PRIVILEGE.getType()),
            "FIELDS_STRUCTURED_PROPERTY"));

    FormService denied = mockFormService(true, false);
    assertThrows(
        CompletionException.class,
        () ->
            submit(
                denied,
                privilegeOnDataset(PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE.getType()),
                "FIELDS_STRUCTURED_PROPERTY"));
    assertNothingSubmitted(denied);
  }

  @Test
  public void testEntityPromptAllowsEntityPropertiesAndDeniesColumnProperties() throws Exception {
    FormService allowed = mockFormService(true, false);
    assertTrue(
        submit(
            allowed,
            privilegeOnDataset(PoliciesConfig.EDIT_ENTITY_PROPERTIES_PRIVILEGE.getType()),
            "STRUCTURED_PROPERTY"));

    FormService denied = mockFormService(true, false);
    assertThrows(
        CompletionException.class,
        () ->
            submit(
                denied,
                privilegeOnDataset(PoliciesConfig.EDIT_DATASET_COL_PROPERTIES_PRIVILEGE.getType()),
                "STRUCTURED_PROPERTY"));
    assertNothingSubmitted(denied);
  }

  @Test
  public void testFieldPromptChecksTheFieldPromptType() throws Exception {
    FormService formService = mockFormService(true, true);

    assertTrue(submit(formService, getMockAllowContext(), "FIELDS_STRUCTURED_PROPERTY"));
    Mockito.verify(formService)
        .isPromptForStructuredProperty(
            any(),
            any(),
            any(),
            eq(PROMPT_ID),
            eq(FormPromptType.FIELDS_STRUCTURED_PROPERTY),
            eq(UrnUtils.getUrn(PROPERTY_URN)));
    Mockito.verify(formService)
        .submitFieldStructuredPropertyPromptResponse(
            any(), any(), any(), any(), any(), eq(PROMPT_ID), eq("field_a"));
  }

  private static QueryContext privilegeOnDataset(String privilege) {
    return getMockAllowContextForResource(
        "urn:li:corpuser:test", privilege, UrnUtils.getUrn(DATASET_URN));
  }

  private static boolean submit(FormService formService, QueryContext context, String promptType)
      throws Exception {
    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getContext()).thenReturn(context);
    Mockito.when(env.getArgument(eq("urn"))).thenReturn(DATASET_URN);
    Mockito.when(env.getArgument(eq("input")))
        .thenReturn(
            Map.of(
                "promptId", PROMPT_ID,
                "formUrn", FORM_URN,
                "type", promptType,
                "fieldPath", "field_a",
                "structuredPropertyParams",
                    Map.of(
                        "structuredPropertyUrn",
                        PROPERTY_URN,
                        "values",
                        List.of(Map.of("stringValue", "answer")))));
    return new SubmitFormPromptResolver(formService).get(env).join();
  }

  private static FormService mockFormService(boolean promptMatches, boolean assigned)
      throws Exception {
    FormService formService = Mockito.mock(FormService.class);
    Mockito.when(
            formService.isPromptForStructuredProperty(
                any(), any(), any(), any(), any(), any(Urn.class)))
        .thenReturn(promptMatches);
    Mockito.when(formService.isFormAssignedToUser(any(), any(), any(), any(), any()))
        .thenReturn(assigned);
    Mockito.when(
            formService.submitStructuredPropertyPromptResponse(
                any(), any(), any(), any(), any(), any()))
        .thenReturn(true);
    Mockito.when(
            formService.submitFieldStructuredPropertyPromptResponse(
                any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(true);
    return formService;
  }

  private static void assertNothingSubmitted(FormService formService) throws Exception {
    Mockito.verify(formService, Mockito.never())
        .submitStructuredPropertyPromptResponse(any(), any(), any(), any(), any(), any());
    Mockito.verify(formService, Mockito.never())
        .submitFieldStructuredPropertyPromptResponse(
            any(), any(), any(), any(), any(), any(), any());
  }
}
