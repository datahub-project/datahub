package com.linkedin.datahub.graphql.types.datajob.mappers;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.domain.Domains;
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

public class DataJobMapperTest {
  private static final Urn TEST_DATA_JOB_URN =
      Urn.createFromTuple(Constants.DATA_JOB_ENTITY_NAME, "datajob1");
  private static final Urn TEST_CONTAINER_URN =
      Urn.createFromTuple(Constants.CONTAINER_ENTITY_NAME, "container1");

  @Test
  public void testMapDataJobContainer() throws URISyntaxException {
    com.linkedin.container.Container input = new com.linkedin.container.Container();
    input.setContainer(TEST_CONTAINER_URN);

    final Map<String, EnvelopedAspect> containerAspect = new HashMap<>();
    containerAspect.put(
        Constants.CONTAINER_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATA_JOB_ENTITY_NAME)
            .setUrn(TEST_DATA_JOB_URN)
            .setAspects(new EnvelopedAspectMap(containerAspect));

    final DataJob actual = DataJobMapper.map(null, response);

    Assert.assertEquals(actual.getUrn(), TEST_DATA_JOB_URN.toString());
    Assert.assertEquals(actual.getContainer().getUrn(), TEST_CONTAINER_URN.toString());
  }

  @Test
  public void testMapDomainsAssociations() throws URISyntaxException {
    final Urn domainUrn1 = Urn.createFromString("urn:li:domain:engineering");
    final Urn domainUrn2 = Urn.createFromString("urn:li:domain:marketing");

    Domains domains = new Domains();
    UrnArray domainUrns = new UrnArray();
    domainUrns.add(domainUrn1);
    domainUrns.add(domainUrn2);
    domains.setDomains(domainUrns);

    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.DOMAINS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(domains.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATA_JOB_ENTITY_NAME)
            .setUrn(TEST_DATA_JOB_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final DataJob actual = DataJobMapper.map(null, response);

    Assert.assertNotNull(actual.getDomainsAssociations());
    Assert.assertEquals(actual.getDomainsAssociations().getDomains().size(), 2);
    Assert.assertEquals(
        actual.getDomainsAssociations().getDomains().get(0).getDomain().getUrn(),
        domainUrn1.toString());
    Assert.assertEquals(
        actual.getDomainsAssociations().getDomains().get(1).getDomain().getUrn(),
        domainUrn2.toString());
  }
}
