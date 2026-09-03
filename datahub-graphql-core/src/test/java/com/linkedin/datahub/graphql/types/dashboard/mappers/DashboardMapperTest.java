package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.common.Access;
import com.linkedin.common.RoleAssociation;
import com.linkedin.common.RoleAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DashboardMapperTest {
  private static final Urn TEST_DASHBOARD_URN =
      Urn.createFromTuple(Constants.DASHBOARD_ENTITY_NAME, "dashboard1");

  @Test
  public void testMapDashboardAccess() throws URISyntaxException {
    final Urn roleUrn = Urn.createFromString("urn:li:role:analyst");
    final RoleAssociationArray roles = new RoleAssociationArray();
    roles.add(new RoleAssociation().setUrn(roleUrn));
    final Access input = new Access().setRoles(roles);

    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.ACCESS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DASHBOARD_ENTITY_NAME)
            .setUrn(TEST_DASHBOARD_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final Dashboard actual = DashboardMapper.map(null, response);

    Assert.assertEquals(actual.getUrn(), TEST_DASHBOARD_URN.toString());
    Assert.assertNotNull(actual.getAccess());
    Assert.assertEquals(actual.getAccess().getRoles().size(), 1);
    // The role's URN is preserved and typed as a ROLE entity, and every association is
    // back-linked to the dashboard it was mapped from (via associatedUrn).
    Assert.assertEquals(
        actual.getAccess().getRoles().get(0).getRole().getUrn(), roleUrn.toString());
    Assert.assertEquals(actual.getAccess().getRoles().get(0).getRole().getType(), EntityType.ROLE);
    Assert.assertEquals(
        actual.getAccess().getRoles().get(0).getAssociatedUrn(), TEST_DASHBOARD_URN.toString());
  }
}
