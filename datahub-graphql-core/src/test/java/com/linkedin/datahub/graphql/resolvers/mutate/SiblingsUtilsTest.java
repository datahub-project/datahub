package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;
import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.SiblingsUtils;
import com.linkedin.metadata.entity.EntityService;
import java.util.HashSet;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SiblingsUtilsTest {

  private static final String TEST_DATASET_URN1 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created,PROD)";
  private static final String TEST_DATASET_URN2 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created2,PROD)";
  private static final String TEST_DATASET_URN3 =
      "urn:li:dataset:(urn:li:dataPlatform:hive,fct_cypress_users_created3,PROD)";

  @Test
  public void testGetSiblingUrns() {
    UrnArray siblingUrns =
        new UrnArray(UrnUtils.getUrn(TEST_DATASET_URN2), UrnUtils.getUrn(TEST_DATASET_URN3));
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockService.getLatestAspect(UrnUtils.getUrn(TEST_DATASET_URN1), SIBLINGS_ASPECT_NAME))
        .thenReturn(new Siblings().setSiblings(siblingUrns));

    assertEquals(
        SiblingsUtils.getSiblingUrns(UrnUtils.getUrn(TEST_DATASET_URN1), mockService), siblingUrns);
  }

  @Test
  public void testGetSiblingUrnsWithoutSiblings() {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockService.getLatestAspect(UrnUtils.getUrn(TEST_DATASET_URN1), SIBLINGS_ASPECT_NAME))
        .thenReturn(new Siblings());

    assertEquals(
        SiblingsUtils.getSiblingUrns(UrnUtils.getUrn(TEST_DATASET_URN1), mockService),
        new UrnArray());
  }

  @Test
  public void testGetSiblingUrnsWithSiblingsAspect() {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockService.getLatestAspect(UrnUtils.getUrn(TEST_DATASET_URN1), SIBLINGS_ASPECT_NAME))
        .thenReturn(null);

    assertEquals(
        SiblingsUtils.getSiblingUrns(UrnUtils.getUrn(TEST_DATASET_URN1), mockService),
        new UrnArray());
  }

  @Test
  public void testGetNextSiblingUrn() {
    UrnArray siblingUrns =
        new UrnArray(UrnUtils.getUrn(TEST_DATASET_URN2), UrnUtils.getUrn(TEST_DATASET_URN3));
    Optional<Urn> nextUrn = SiblingsUtils.getNextSiblingUrn(siblingUrns, new HashSet<>());

    assertEquals(nextUrn, Optional.of(UrnUtils.getUrn(TEST_DATASET_URN2)));
  }

  @Test
  public void testGetNextSiblingUrnWithUsedUrns() {
    UrnArray siblingUrns =
        new UrnArray(UrnUtils.getUrn(TEST_DATASET_URN2), UrnUtils.getUrn(TEST_DATASET_URN3));
    HashSet<Urn> usedUrns = new HashSet<>();
    usedUrns.add(UrnUtils.getUrn(TEST_DATASET_URN2));
    Optional<Urn> nextUrn = SiblingsUtils.getNextSiblingUrn(siblingUrns, usedUrns);

    assertEquals(nextUrn, Optional.of(UrnUtils.getUrn(TEST_DATASET_URN3)));
  }

  @Test
  public void testGetNextSiblingUrnNoSiblings() {
    Optional<Urn> nextUrn = SiblingsUtils.getNextSiblingUrn(new UrnArray(), new HashSet<>());

    assertEquals(nextUrn, Optional.empty());
  }
}
