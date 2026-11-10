package com.linkedin.datahub.graphql.types.application.mappers;

import static com.linkedin.metadata.Constants.APPLICATION_PROPERTIES_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.Application;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class ApplicationMapperTest {

  private static final Urn APPLICATION_URN = UrnUtils.getUrn("urn:li:application:child");
  private static final Urn PARENT_APPLICATION_URN = UrnUtils.getUrn("urn:li:application:parent");

  @Test
  public void testMapsParentApplicationStub() {
    ApplicationProperties properties = new ApplicationProperties().setName("Child");
    properties.setParentApplication(PARENT_APPLICATION_URN);

    Application result = ApplicationMapper.map(null, entityResponse(properties));

    assertNotNull(result.getParentApplication());
    assertEquals(result.getParentApplication().getUrn(), PARENT_APPLICATION_URN.toString());
    assertEquals(result.getParentApplication().getType(), EntityType.APPLICATION);
  }

  @Test
  public void testNoParentApplication() {
    ApplicationProperties properties = new ApplicationProperties().setName("Child");

    Application result = ApplicationMapper.map(null, entityResponse(properties));

    assertNull(result.getParentApplication());
  }

  private static EntityResponse entityResponse(ApplicationProperties properties) {
    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        APPLICATION_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(properties.data())));
    return new EntityResponse()
        .setUrn(APPLICATION_URN)
        .setEntityName("application")
        .setAspects(new EnvelopedAspectMap(aspects));
  }
}
