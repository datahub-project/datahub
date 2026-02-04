package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.VersionedDataset;
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

public class VersionedDatasetMapperTest {
  private static final Urn TEST_DATASET_URN =
      Urn.createFromTuple(Constants.DATASET_ENTITY_NAME, "dataset1");

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
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final VersionedDataset actual = VersionedDatasetMapper.map(null, response);

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
