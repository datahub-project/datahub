package com.linkedin.datahub.graphql.types.tag.mappers;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.linkedin.common.Deprecation;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class TagMapperTest {

  private static final String TEST_TAG_URN = "urn:li:tag:SomeTag";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:datahub";

  @Test
  public void testMapTagBasic() throws Exception {
    Urn tagUrn = Urn.createFromString(TEST_TAG_URN);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(tagUrn);
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));

    Tag result = TagMapper.map(null, entityResponse);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_TAG_URN);
    assertEquals(result.getType(), EntityType.TAG);
    assertNull(result.getDeprecation());
  }

  @Test
  public void testMapTagWithDeprecation() throws Exception {
    Urn tagUrn = Urn.createFromString(TEST_TAG_URN);
    Urn actorUrn = Urn.createFromString(TEST_ACTOR_URN);

    Deprecation deprecation = new Deprecation();
    deprecation.setDeprecated(true);
    deprecation.setNote("This tag is deprecated.");
    deprecation.setActor(actorUrn);

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        DEPRECATION_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(deprecation.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(tagUrn);
    entityResponse.setAspects(new EnvelopedAspectMap(aspects));

    Tag result = TagMapper.map(null, entityResponse);

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_TAG_URN);
    assertNotNull(result.getDeprecation());
    assertTrue(result.getDeprecation().getDeprecated());
    assertEquals(result.getDeprecation().getNote(), "This tag is deprecated.");
    assertEquals(result.getDeprecation().getActor(), TEST_ACTOR_URN);
  }
}
