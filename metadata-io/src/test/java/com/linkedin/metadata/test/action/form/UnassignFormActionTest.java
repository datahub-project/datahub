package com.linkedin.metadata.test.action.form;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.FormServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UnassignFormActionTest {

  @Test
  public void testValidateWithMissingFormUrnParameter() {
    FormServiceAsync formService = Mockito.mock(FormServiceAsync.class);
    UnassignFormAction formAction = new UnassignFormAction(formService);
    // Given
    ActionParameters params = new ActionParameters(new HashMap<>());
    // When/Then
    assertThrows(InvalidActionParamsException.class, () -> formAction.validate(params));
  }

  @Test
  public void testValidateValidParameters() {
    FormServiceAsync formService = Mockito.mock(FormServiceAsync.class);
    UnassignFormAction formAction = new UnassignFormAction(formService);
    // Given
    ActionParameters params =
        new ActionParameters(ImmutableMap.of("formUrn", ImmutableList.of("urn:li:form:test")));
    formAction.validate(params); // No exception
  }

  @Test
  public void testApplyCallsFormService() throws Exception {
    FormServiceAsync formService = Mockito.mock(FormServiceAsync.class);
    UnassignFormAction formAction = new UnassignFormAction(formService);
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

    // Given
    Map<String, List<String>> paramsMap = new HashMap<>();
    paramsMap.put("formUrn", Collections.singletonList(formUrn.toString()));
    ActionParameters params = new ActionParameters(paramsMap);

    formAction.apply(mock(OperationContext.class), Collections.singletonList(entityUrn), params);
    verify(formService, times(1))
        .batchUnassignFormForEntities(
            any(OperationContext.class), eq(Collections.singletonList(entityUrn)), eq(formUrn));
  }
}
