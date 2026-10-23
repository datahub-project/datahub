package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.assertTrue;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnerMapperTest {

  private static final Urn ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlings.reset();
  }

  private com.linkedin.common.Owner pegasusOwnerFor(String ownerUrn) {
    return new com.linkedin.common.Owner()
        .setOwner(UrnUtils.getUrn(ownerUrn))
        .setType(OwnershipType.TECHNICAL_OWNER);
  }

  @Test
  public void testOwnerOfOtherRegisteredTypeStillMapsAsGroup() {
    // Only entity types the registry doesn't know are skipped; other registered types keep
    // rendering as before.
    Owner result =
        OwnerMapper.map(
            TestUtils.getMockAllowContext(),
            pegasusOwnerFor("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)"),
            ENTITY_URN);
    assertTrue(result.getOwner() instanceof CorpGroup);
  }
}
