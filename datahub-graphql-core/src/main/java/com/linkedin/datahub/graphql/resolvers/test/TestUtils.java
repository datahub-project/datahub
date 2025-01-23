package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;

import com.datahub.authorization.AuthUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.TestDefinitionInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import java.util.Map;
import javax.annotation.Nonnull;

public class TestUtils {

  /** Returns true if the authenticated user is able to view tests. */
  public static boolean canViewTests(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.VIEW_TESTS_PRIVILEGE);
  }

  /** Returns true if the authenticated user is able to manage tests. */
  public static boolean canManageTests(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getOperationContext(), PoliciesConfig.MANAGE_TESTS_PRIVILEGE);
  }

  public static TestDefinition mapDefinition(final TestDefinitionInput testDefInput) {
    final TestDefinition result = new TestDefinition();
    result.setType(TestDefinitionType.JSON); // Always JSON for now.
    result.setJson(testDefInput.getJson(), SetMode.IGNORE_NULL);
    return result;
  }

  public static EntityResponse buildEntityResponse(Map<String, RecordTemplate> aspects) {
    final EntityResponse entityResponse = new EntityResponse();
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    for (Map.Entry<String, RecordTemplate> entry : aspects.entrySet()) {
      aspectMap.put(
          entry.getKey(), new EnvelopedAspect().setValue(new Aspect(entry.getValue().data())));
    }
    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }

  private TestUtils() {}
}
