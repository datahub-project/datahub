package com.linkedin.datahub.graphql.types.test;

import com.linkedin.datahub.graphql.generated.TestDefinition;
import com.linkedin.test.TestInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;


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
    } else  {
      return null;
    }
    return result;
  }

  private TestMapper() {
  }
}