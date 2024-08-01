package com.linkedin.metadata.test.action.form;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SubmitFormPromptActionTest {

  @Test
  public void testValidateWithInvalidParameters() {
    FormService formService = Mockito.mock(FormService.class);
    SubmitFormPromptAction submitPromptAction = new SubmitFormPromptAction(formService);
    Map<String, List<String>> paramsMap = new HashMap<>();

    // missing everything
    ActionParameters missingParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class, () -> submitPromptAction.validate(missingParams));

    // missing actorUrn
    paramsMap.put("formUrn", ImmutableList.of("test"));
    paramsMap.put("promptId", ImmutableList.of("test"));
    paramsMap.put("promptType", ImmutableList.of("STRUCTURED_PROPERTY"));
    ActionParameters missingActorUrnParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class,
        () -> submitPromptAction.validate(missingActorUrnParams));

    // missing formUrn
    paramsMap.remove("formUrn");
    paramsMap.put("actorUrn", ImmutableList.of("test"));
    paramsMap.put("promptId", ImmutableList.of("test"));
    paramsMap.put("promptType", ImmutableList.of("STRUCTURED_PROPERTY"));
    ActionParameters missingFormUrnParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class,
        () -> submitPromptAction.validate(missingFormUrnParams));

    // missing promptId
    paramsMap.remove("promptId");
    paramsMap.put("actorUrn", ImmutableList.of("test"));
    paramsMap.put("formUrn", ImmutableList.of("test"));
    paramsMap.put("promptType", ImmutableList.of("STRUCTURED_PROPERTY"));
    ActionParameters missingPromptIdParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class,
        () -> submitPromptAction.validate(missingPromptIdParams));

    // missing promptType
    paramsMap.remove("promptType");
    paramsMap.put("actorUrn", ImmutableList.of("test"));
    paramsMap.put("formUrn", ImmutableList.of("test"));
    paramsMap.put("promptId", ImmutableList.of("test"));
    ActionParameters missingPromptTypeParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class,
        () -> submitPromptAction.validate(missingPromptTypeParams));

    // invalid promptType
    paramsMap.put("promptType", ImmutableList.of("invalid type"));
    ActionParameters invalidPromptTypeParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class,
        () -> submitPromptAction.validate(invalidPromptTypeParams));
  }

  @Test
  public void testValidateValidParameters() {
    FormService formService = Mockito.mock(FormService.class);
    SubmitFormPromptAction submitPromptAction = new SubmitFormPromptAction(formService);
    ActionParameters params =
        new ActionParameters(
            ImmutableMap.of(
                "actorUrn",
                ImmutableList.of("urn:li:corpuser:test"),
                "formUrn",
                ImmutableList.of("urn:li:form:test"),
                "promptId",
                ImmutableList.of("test"),
                "promptType",
                ImmutableList.of("STRUCTURED_PROPERTY")));
    submitPromptAction.validate(params); // No exception
  }

  @Test
  public void testApplyStructuredPropertyPromptStringValues() throws Exception {
    FormService formService = Mockito.mock(FormService.class);
    SubmitFormPromptAction submitPromptAction = new SubmitFormPromptAction(formService);
    Urn actorUrn = UrnUtils.getUrn("urn:li:corpuser:testing");
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    String promptId = "testPrompt123";
    String promptType = "STRUCTURED_PROPERTY";
    Urn structuredPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:1");
    List<String> stringValues = ImmutableList.of("testValue1");

    // Given
    Map<String, List<String>> paramsMap = new HashMap<>();
    paramsMap.put("actorUrn", Collections.singletonList(actorUrn.toString()));
    paramsMap.put("formUrn", Collections.singletonList(formUrn.toString()));
    paramsMap.put("promptId", Collections.singletonList(promptId));
    paramsMap.put("promptType", Collections.singletonList(promptType));
    paramsMap.put(
        "structuredPropertyUrn", Collections.singletonList(structuredPropertyUrn.toString()));
    paramsMap.put("stringValues", stringValues);
    ActionParameters params = new ActionParameters(paramsMap);

    submitPromptAction.apply(
        mock(OperationContext.class), Collections.singletonList(entityUrn), params);

    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create(stringValues.get(0)));
    verify(formService, times(1))
        .batchSubmitStructuredPropertyPromptResponse(
            any(OperationContext.class),
            eq(Collections.singletonList(entityUrn.toString())),
            eq(structuredPropertyUrn),
            eq(values),
            eq(formUrn),
            eq(promptId),
            eq(actorUrn),
            eq(false));
  }

  @Test
  public void testApplyStructuredPropertyPromptNumberValues() throws Exception {
    FormService formService = Mockito.mock(FormService.class);
    SubmitFormPromptAction submitPromptAction = new SubmitFormPromptAction(formService);
    Urn actorUrn = UrnUtils.getUrn("urn:li:corpuser:testing");
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    String promptId = "testPrompt123";
    String promptType = "STRUCTURED_PROPERTY";
    Urn structuredPropertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:1");
    List<String> numberValues = ImmutableList.of("123", "1.23");

    // Given
    Map<String, List<String>> paramsMap = new HashMap<>();
    paramsMap.put("actorUrn", Collections.singletonList(actorUrn.toString()));
    paramsMap.put("formUrn", Collections.singletonList(formUrn.toString()));
    paramsMap.put("promptId", Collections.singletonList(promptId));
    paramsMap.put("promptType", Collections.singletonList(promptType));
    paramsMap.put(
        "structuredPropertyUrn", Collections.singletonList(structuredPropertyUrn.toString()));
    paramsMap.put("numberValues", numberValues);
    ActionParameters params = new ActionParameters(paramsMap);

    submitPromptAction.apply(
        mock(OperationContext.class), Collections.singletonList(entityUrn), params);

    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create(Double.parseDouble(numberValues.get(0))));
    values.add(PrimitivePropertyValue.create(Double.parseDouble(numberValues.get(1))));
    verify(formService, times(1))
        .batchSubmitStructuredPropertyPromptResponse(
            any(OperationContext.class),
            eq(Collections.singletonList(entityUrn.toString())),
            eq(structuredPropertyUrn),
            eq(values),
            eq(formUrn),
            eq(promptId),
            eq(actorUrn),
            eq(false));
  }

  @Test
  public void testApplyOwnershipPrompt() throws Exception {
    FormService formService = Mockito.mock(FormService.class);
    SubmitFormPromptAction submitPromptAction = new SubmitFormPromptAction(formService);
    Urn actorUrn = UrnUtils.getUrn("urn:li:corpuser:testing");
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    String promptId = "testPrompt123";
    String promptType = "OWNERSHIP";
    List<String> owners = ImmutableList.of("urn:li:corpuser:admin");
    String ownershipTypeUrn = "urn:li:ownershipType:test";

    // Given
    Map<String, List<String>> paramsMap = new HashMap<>();
    paramsMap.put("actorUrn", Collections.singletonList(actorUrn.toString()));
    paramsMap.put("formUrn", Collections.singletonList(formUrn.toString()));
    paramsMap.put("promptId", Collections.singletonList(promptId));
    paramsMap.put("promptType", Collections.singletonList(promptType));
    paramsMap.put("owners", owners);
    paramsMap.put("ownershipTypeUrn", Collections.singletonList(ownershipTypeUrn));
    ActionParameters params = new ActionParameters(paramsMap);

    submitPromptAction.apply(
        mock(OperationContext.class), Collections.singletonList(entityUrn), params);
    verify(formService, times(1))
        .batchSubmitOwnershipPromptResponse(
            any(OperationContext.class),
            eq(Collections.singletonList(entityUrn.toString())),
            eq(owners.stream().map(UrnUtils::getUrn).collect(Collectors.toList())),
            eq(UrnUtils.getUrn(ownershipTypeUrn)),
            eq(formUrn),
            eq(promptId),
            eq(actorUrn),
            eq(false));
  }
}
