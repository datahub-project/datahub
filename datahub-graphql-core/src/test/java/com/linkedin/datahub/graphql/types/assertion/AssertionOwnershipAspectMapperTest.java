package com.linkedin.datahub.graphql.types.assertion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.HashMap;
import org.testng.annotations.Test;

public class AssertionOwnershipAspectMapperTest {

  @Test
  public void testApplyOwnershipIfPresent() {
    Ownership ownership =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    ImmutableList.of(
                        new Owner()
                            .setOwner(UrnUtils.getUrn("urn:li:corpuser:direct_test_owner"))
                            .setType(OwnershipType.DATAOWNER))))
            .setLastModified(
                new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:actor")));

    HashMap<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(Constants.OWNERSHIP_ASPECT_NAME, envelopedAspect(ownership.data()));

    Assertion result = new Assertion();
    AssertionOwnershipAspectMapper.applyOwnershipIfPresent(
        null,
        UrnUtils.getUrn("urn:li:assertion:mapper-test"),
        new EnvelopedAspectMap(aspects),
        result);

    assertNotNull(result.getOwnership());
    assertEquals(result.getOwnership().getOwners().size(), 1);
    assertEquals(
        ((Entity) result.getOwnership().getOwners().get(0).getOwner()).getUrn(),
        "urn:li:corpuser:direct_test_owner");
  }

  private static EnvelopedAspect envelopedAspect(DataMap data) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(data));
    return envelopedAspect;
  }
}
