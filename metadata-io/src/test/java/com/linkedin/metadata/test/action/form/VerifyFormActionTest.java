package com.linkedin.metadata.test.action.form;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

public class VerifyFormActionTest {

  @Test
  public void testValidateWithInvalidParameters() {
    FormServiceAsync formService = Mockito.mock(FormServiceAsync.class);
    VerifyFormAction verifyFormAction = new VerifyFormAction(formService);
    Map<String, List<String>> paramsMap = new HashMap<>();

    // missing everything
    ActionParameters missingParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class, () -> verifyFormAction.validate(missingParams));

    // missing actorUrn
    paramsMap.put("formUrn", ImmutableList.of("test"));
    ActionParameters missingActorUrnParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class, () -> verifyFormAction.validate(missingActorUrnParams));

    // missing formUrn
    paramsMap.remove("formUrn");
    paramsMap.put("actorUrn", ImmutableList.of("test"));
    ActionParameters missingFormUrnParams = new ActionParameters(paramsMap);
    assertThrows(
        InvalidActionParamsException.class, () -> verifyFormAction.validate(missingFormUrnParams));
  }

  @Test
  public void testValidateValidParameters() {
    FormServiceAsync formService = Mockito.mock(FormServiceAsync.class);
    VerifyFormAction verifyFormAction = new VerifyFormAction(formService);
    ActionParameters params =
        new ActionParameters(
            ImmutableMap.of(
                "actorUrn",
                ImmutableList.of("urn:li:corpuser:test"),
                "formUrn",
                ImmutableList.of("urn:li:form:test")));
    verifyFormAction.validate(params); // No exception
  }

  @Test
  public void testVerifyFormAction() throws Exception {
    FormServiceAsync formService = Mockito.mock(FormServiceAsync.class);
    VerifyFormAction verifyFormAction = new VerifyFormAction(formService);
    Urn actorUrn = UrnUtils.getUrn("urn:li:corpuser:testing");
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test");
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

    // Given
    Map<String, List<String>> paramsMap = new HashMap<>();
    paramsMap.put("actorUrn", Collections.singletonList(actorUrn.toString()));
    paramsMap.put("formUrn", Collections.singletonList(formUrn.toString()));
    ActionParameters params = new ActionParameters(paramsMap);

    verifyFormAction.apply(
        mock(OperationContext.class), Collections.singletonList(entityUrn), params);

    verify(formService, times(1))
        .batchVerifyForm(
            any(OperationContext.class),
            eq(Collections.singletonList(entityUrn.toString())),
            eq(formUrn),
            eq(actorUrn));
  }
}
