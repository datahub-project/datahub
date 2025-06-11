package com.linkedin.datahub.graphql.types.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.TestDefinition;
import com.linkedin.datahub.graphql.generated.TestMode;
import com.linkedin.datahub.graphql.generated.TestStatus;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.test.TestInfo;

public class TestMapper {

  public static Test map(final EntityResponse entityResponse) {

    final Test result = new Test();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.TEST);

    final EnvelopedAspect envelopedTestInfo = aspects.get(Constants.TEST_INFO_ASPECT_NAME);
    if (envelopedTestInfo != null) {
      final TestInfo testInfo = new TestInfo(envelopedTestInfo.getValue().data());
      result.setCategory(testInfo.getCategory());
      result.setName(testInfo.getName());
      result.setDescription(testInfo.getDescription());
      result.setDefinition(new TestDefinition(testInfo.getDefinition().getJson()));
      if (testInfo.getStatus() != null) {
        result.setStatus(mapStatus(testInfo.getStatus()));
      }
    } else {
      return null;
    }
    return result;
  }

  private static TestStatus mapStatus(com.linkedin.test.TestStatus gmsStatus) {
    TestStatus status = new TestStatus();
    status.setMode(TestMode.valueOf(gmsStatus.getMode().toString()));
    return status;
  }

  private TestMapper() {}
}
