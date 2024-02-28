package com.linkedin.metadata.test.action.form;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UnassignFormActionTest {

  @Test
  public void testValidateWithMissingFormUrnParameter() {
    FormService formService = Mockito.mock(FormService.class);
    UnassignFormAction formAction = new UnassignFormAction(formService);
    // Given
    ActionParameters params = new ActionParameters(new HashMap<>());
    // When/Then
    assertThrows(InvalidActionParamsException.class, () -> formAction.validate(params));
  }

  @Test
  public void testValidateValidParameters() {
    FormService formService = Mockito.mock(FormService.class);
    UnassignFormAction formAction = new UnassignFormAction(formService);
    // Given
    ActionParameters params =
        new ActionParameters(ImmutableMap.of("formUrn", ImmutableList.of("urn:li:form:test")));
    formAction.validate(params); // No exception
  }

  @Test
  public void testApplyCallsFormService() throws Exception {
    FormService formService = Mockito.mock(FormService.class);
    UnassignFormAction formAction = new UnassignFormAction(formService);
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

    // Given
    Map<String, List<String>> paramsMap = new HashMap<>();
    paramsMap.put("formUrn", Collections.singletonList(formUrn.toString()));
    ActionParameters params = new ActionParameters(paramsMap);

    formAction.apply(Collections.singletonList(entityUrn), params);
    verify(formService, times(1))
        .batchUnassignFormForEntities(Collections.singletonList(entityUrn), formUrn);
  }
}
