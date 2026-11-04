package com.linkedin.datahub.graphql.types.common.mappers;

import static com.linkedin.metadata.Constants.API_ENTITY_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Api;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import org.testng.annotations.Test;

public class UrnToEntityMapperTest {

  @Test
  public void testMapApi() throws Exception {
    Urn urn = Urn.createFromString("urn:li:" + API_ENTITY_NAME + ":payments.charge");
    Entity result = UrnToEntityMapper.map(null, urn);
    assertNotNull(result);
    assertTrue(result instanceof Api);
    assertEquals(result.getUrn(), urn.toString());
    assertEquals(((Api) result).getType(), EntityType.API);
  }

  @Test
  public void testUnknownEntityTypeReturnsNull() throws Exception {
    Urn urn = Urn.createFromString("urn:li:unknownEntityXyz:foo");
    Entity result = UrnToEntityMapper.map(null, urn);
    assertNull(result);
  }
}
