package com.linkedin.datahub.graphql.types.entitytype;

import static com.linkedin.metadata.Constants.ENTITY_TYPE_INFO_ASPECT_NAME;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.EntityTypeEntity;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entitytype.EntityTypeInfo;
import java.util.Map;
import org.testng.annotations.Test;

public class EntityTypeEntityMapperTest {

  @Test
  public void testKnownEntityTypeMapsToItsType() {
    EntityTypeEntity result =
        EntityTypeEntityMapper.map(null, entityTypeResponse("urn:li:entityType:datahub.dataset"));

    assertEquals(result.getInfo().getType(), EntityType.DATASET);
  }

  @Test
  public void testEntityTypeUnknownToThisBuildMapsToOther() {
    // After a version rollback, the newer build's bootstrap has already written entityType
    // entities for types this build does not know. Mapping them must not fail the batch.
    EntityTypeEntity result =
        EntityTypeEntityMapper.map(
            null, entityTypeResponse("urn:li:entityType:datahub.entityFromNewerBuild"));

    assertEquals(result.getUrn(), "urn:li:entityType:datahub.entityFromNewerBuild");
    assertEquals(result.getInfo().getQualifiedName(), "datahub.entityFromNewerBuild");
    assertEquals(result.getInfo().getType(), EntityType.OTHER);
  }

  private static EntityResponse entityTypeResponse(String urn) {
    EntityTypeInfo info =
        new EntityTypeInfo().setQualifiedName(urn.substring("urn:li:entityType:".length()));
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(info.data()));
    return new EntityResponse()
        .setUrn(UrnUtils.getUrn(urn))
        .setEntityName("entityType")
        .setAspects(new EnvelopedAspectMap(Map.of(ENTITY_TYPE_INFO_ASPECT_NAME, aspect)));
  }
}
