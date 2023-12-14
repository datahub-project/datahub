package com.linkedin.metadata.graph.sibling;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.SiblingGraphService;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SiblingGraphServiceTest {

  /** Some test URN types. */
  protected static String datasetType = "dataset";

  /** Some test datasets. */
  protected static String datasetOneUrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetOne,PROD)";

  protected static String datasetTwoUrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetTwo,PROD)";
  protected static String datasetThreeUrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetThree,PROD)";
  protected static String datasetFourUrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetFour,PROD)";
  protected static String datasetFiveUrnString =
      "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetFive,PROD)";

  protected static Urn datasetOneUrn = createFromString(datasetOneUrnString);
  protected static Urn datasetTwoUrn = createFromString(datasetTwoUrnString);
  protected static Urn datasetThreeUrn = createFromString(datasetThreeUrnString);
  protected static Urn datasetFourUrn = createFromString(datasetFourUrnString);
  protected static Urn datasetFiveUrn = createFromString(datasetFiveUrnString);

  /** Some test relationships. */
  protected static String downstreamOf = "DownstreamOf";

  protected static String upstreamOf = "UpstreamOf";

  private GraphService _graphService;
  private SiblingGraphService _client;
  EntityService _mockEntityService;

  @BeforeClass
  public void setup() {
    _mockEntityService = Mockito.mock(EntityService.class);
    when(_mockEntityService.exists(any())).thenReturn(true);
    _graphService = Mockito.mock(GraphService.class);
    _client = new SiblingGraphService(_mockEntityService, _graphService);
  }

  @Test
  public void testNoSiblingMetadata() {
    EntityLineageResult mockResult = new EntityLineageResult();
    LineageRelationshipArray relationships = new LineageRelationshipArray();
    LineageRelationship relationship1 = new LineageRelationship();
    relationship1.setDegree(0);
    relationship1.setType(downstreamOf);
    relationship1.setEntity(datasetOneUrn);

    LineageRelationship relationship2 = new LineageRelationship();
    relationship2.setDegree(0);
    relationship2.setType(downstreamOf);
    relationship2.setEntity(datasetTwoUrn);

    LineageRelationship relationship3 = new LineageRelationship();
    relationship3.setDegree(0);
    relationship3.setType(downstreamOf);
    relationship3.setEntity(datasetThreeUrn);

    relationships.add(relationship1);
    relationships.add(relationship2);
    relationships.add(relationship3);

    mockResult.setStart(0);
    mockResult.setTotal(200);
    mockResult.setCount(3);
    mockResult.setFiltered(0);
    mockResult.setRelationships(relationships);

    when(_graphService.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1, null, null))
        .thenReturn(mockResult);

    when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME)).thenReturn(null);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage =
        service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert sibling graph service is a pass through in the case that there is no sibling metadataa
    assertEquals(upstreamLineage, mockResult);
  }

  @Test
  public void testNoSiblingInResults() {
    EntityLineageResult mockResult = new EntityLineageResult();
    EntityLineageResult siblingMockResult = new EntityLineageResult();

    LineageRelationshipArray relationships = new LineageRelationshipArray();
    LineageRelationship relationship1 = new LineageRelationship();
    relationship1.setDegree(0);
    relationship1.setType(downstreamOf);
    relationship1.setEntity(datasetOneUrn);

    LineageRelationship relationship2 = new LineageRelationship();
    relationship2.setDegree(0);
    relationship2.setType(downstreamOf);
    relationship2.setEntity(datasetTwoUrn);

    LineageRelationship relationship3 = new LineageRelationship();
    relationship3.setDegree(0);
    relationship3.setType(downstreamOf);
    relationship3.setEntity(datasetThreeUrn);

    relationships.add(relationship1);
    relationships.add(relationship2);
    relationships.add(relationship3);

    mockResult.setStart(0);
    mockResult.setTotal(200);
    mockResult.setCount(3);
    mockResult.setFiltered(0);
    mockResult.setRelationships(relationships);

    when(_graphService.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1, null, null))
        .thenReturn(mockResult);

    siblingMockResult.setStart(0);
    siblingMockResult.setTotal(0);
    siblingMockResult.setCount(0);
    siblingMockResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getLineage(datasetFiveUrn, LineageDirection.UPSTREAM, 0, 97, 1, null, null))
        .thenReturn(siblingMockResult);

    Siblings noRelevantSiblingsResponse = new Siblings();
    noRelevantSiblingsResponse.setPrimary(true);
    noRelevantSiblingsResponse.setSiblings(new UrnArray(ImmutableList.of(datasetFiveUrn)));

    when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(noRelevantSiblingsResponse);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Map<Urn, List<RecordTemplate>> siblingsMap =
        ImmutableMap.of(
            datasetOneUrn, ImmutableList.of(dataset1Siblings),
            datasetTwoUrn, ImmutableList.of(dataset2Siblings),
            datasetThreeUrn, ImmutableList.of(dataset3Siblings));

    when(_mockEntityService.getLatestAspects(any(), any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage =
        service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert sibling graph service is a pass through in the case that your sibling has no lineage
    assertEquals(upstreamLineage, mockResult);
  }

  @Test
  public void testSiblingInResult() throws Exception {
    EntityLineageResult mockResult = new EntityLineageResult();
    EntityLineageResult siblingMockResult = new EntityLineageResult();

    LineageRelationshipArray relationships = new LineageRelationshipArray();
    LineageRelationship relationship1 = new LineageRelationship();
    relationship1.setDegree(0);
    relationship1.setType(downstreamOf);
    relationship1.setEntity(datasetOneUrn);

    LineageRelationship relationship2 = new LineageRelationship();
    relationship2.setDegree(0);
    relationship2.setType(downstreamOf);
    relationship2.setEntity(datasetTwoUrn);

    LineageRelationship relationship3 = new LineageRelationship();
    relationship3.setDegree(0);
    relationship3.setType(downstreamOf);
    relationship3.setEntity(datasetThreeUrn);

    relationships.add(relationship1);
    relationships.add(relationship2);
    relationships.add(relationship3);

    mockResult.setStart(0);
    mockResult.setTotal(3);
    mockResult.setCount(3);
    mockResult.setRelationships(relationships);

    siblingMockResult.setStart(0);
    siblingMockResult.setTotal(0);
    siblingMockResult.setCount(0);
    siblingMockResult.setRelationships(new LineageRelationshipArray());

    when(_graphService.getLineage(datasetThreeUrn, LineageDirection.UPSTREAM, 0, 98, 1, null, null))
        .thenReturn(siblingMockResult);

    when(_graphService.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1, null, null))
        .thenReturn(mockResult);

    Siblings siblingInSearchResult = new Siblings();
    siblingInSearchResult.setPrimary(true);
    siblingInSearchResult.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(siblingInSearchResult);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Map<Urn, List<RecordTemplate>> siblingsMap =
        ImmutableMap.of(
            datasetOneUrn, ImmutableList.of(dataset1Siblings),
            datasetTwoUrn, ImmutableList.of(dataset2Siblings),
            datasetThreeUrn, ImmutableList.of(dataset3Siblings));

    when(_mockEntityService.getLatestAspects(any(), any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult expectedResult = mockResult.clone();
    expectedResult.setTotal(3);
    expectedResult.setCount(2);
    expectedResult.setFiltered(1);
    expectedResult.setRelationships(new LineageRelationshipArray(relationship1, relationship2));

    EntityLineageResult upstreamLineage =
        service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert your sibling will be filtered out of your lineage
    assertEquals(upstreamLineage, expectedResult);
    assertEquals(upstreamLineage.getRelationships().size(), 2);
  }

  @Test
  public void testCombineSiblingResult() {
    EntityLineageResult mockResult = new EntityLineageResult();
    EntityLineageResult siblingMockResult = new EntityLineageResult();
    EntityLineageResult expectedResult = new EntityLineageResult();

    LineageRelationshipArray relationships = new LineageRelationshipArray();
    LineageRelationshipArray siblingRelationships = new LineageRelationshipArray();
    LineageRelationshipArray expectedRelationships = new LineageRelationshipArray();

    LineageRelationship relationship1 = new LineageRelationship();
    relationship1.setDegree(0);
    relationship1.setType(downstreamOf);
    relationship1.setEntity(datasetOneUrn);

    LineageRelationship relationship2 = new LineageRelationship();
    relationship2.setDegree(0);
    relationship2.setType(downstreamOf);
    relationship2.setEntity(datasetTwoUrn);

    LineageRelationship relationship3 = new LineageRelationship();
    relationship3.setDegree(0);
    relationship3.setType(downstreamOf);
    relationship3.setEntity(datasetThreeUrn);

    LineageRelationship relationship4 = new LineageRelationship();
    relationship4.setDegree(0);
    relationship4.setType(downstreamOf);
    relationship4.setEntity(datasetFiveUrn);

    relationships.add(relationship1);

    expectedRelationships.add(relationship2);
    expectedRelationships.add(relationship4);
    expectedRelationships.add(
        relationship1); // expect just one relationship1 despite duplicates in sibling lineage

    expectedResult.setCount(3);
    expectedResult.setStart(0);
    expectedResult.setTotal(4);
    expectedResult.setFiltered(1);
    expectedResult.setRelationships(expectedRelationships);

    mockResult.setStart(0);
    mockResult.setTotal(1);
    mockResult.setCount(1);
    mockResult.setRelationships(relationships);

    siblingRelationships.add(relationship2);
    siblingRelationships.add(relationship4);
    siblingRelationships.add(
        relationship1); // duplicate from sibling's lineage, we should not see duplicates in result
    siblingMockResult.setStart(0);
    siblingMockResult.setTotal(3);
    siblingMockResult.setCount(2);
    siblingMockResult.setRelationships(siblingRelationships);

    when(_graphService.getLineage(
            Mockito.eq(datasetThreeUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .then(invocation -> siblingMockResult.clone());

    when(_graphService.getLineage(
            Mockito.eq(datasetFourUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .then(invocation -> mockResult.clone());

    Siblings siblingInSearchResult = new Siblings();
    siblingInSearchResult.setPrimary(true);
    siblingInSearchResult.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(siblingInSearchResult);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Map<Urn, List<RecordTemplate>> siblingsMap =
        ImmutableMap.of(
            datasetOneUrn, ImmutableList.of(dataset1Siblings),
            datasetTwoUrn, ImmutableList.of(dataset2Siblings),
            datasetThreeUrn, ImmutableList.of(dataset3Siblings),
            datasetFiveUrn, ImmutableList.of(dataset3Siblings));

    when(_mockEntityService.getLatestAspects(any(), any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage =
        service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert your lineage will be combined with your siblings lineage
    assertEquals(upstreamLineage, expectedResult);
  }

  @Test
  public void testUpstreamOfSiblings() {
    EntityLineageResult mockResult = new EntityLineageResult();
    EntityLineageResult siblingMockResult = new EntityLineageResult();
    EntityLineageResult expectedResult = new EntityLineageResult();

    LineageRelationshipArray relationships = new LineageRelationshipArray();
    LineageRelationshipArray siblingRelationships = new LineageRelationshipArray();
    LineageRelationshipArray expectedRelationships = new LineageRelationshipArray();

    LineageRelationship relationship1 = new LineageRelationship();
    relationship1.setDegree(0);
    relationship1.setType(downstreamOf);
    relationship1.setEntity(datasetOneUrn);

    LineageRelationship relationship2 = new LineageRelationship();
    relationship2.setDegree(0);
    relationship2.setType(downstreamOf);
    relationship2.setEntity(datasetTwoUrn);

    LineageRelationship relationship3 = new LineageRelationship();
    relationship3.setDegree(0);
    relationship3.setType(downstreamOf);
    relationship3.setEntity(datasetThreeUrn);

    LineageRelationship relationship5 = new LineageRelationship();
    relationship5.setDegree(0);
    relationship5.setType(downstreamOf);
    relationship5.setEntity(datasetFiveUrn);

    relationships.add(relationship1);

    expectedRelationships.add(relationship5);
    expectedRelationships.add(relationship1);

    expectedResult.setCount(2);
    expectedResult.setStart(0);
    expectedResult.setTotal(3);
    expectedResult.setFiltered(1);
    expectedResult.setRelationships(expectedRelationships);

    mockResult.setStart(0);
    mockResult.setTotal(1);
    mockResult.setCount(1);
    mockResult.setRelationships(relationships);

    siblingRelationships.add(relationship2);
    siblingRelationships.add(relationship5);
    siblingMockResult.setStart(0);
    siblingMockResult.setTotal(2);
    siblingMockResult.setCount(2);
    siblingMockResult.setRelationships(siblingRelationships);

    when(_graphService.getLineage(datasetThreeUrn, LineageDirection.UPSTREAM, 0, 99, 1, null, null))
        .thenReturn(siblingMockResult);

    when(_graphService.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1, null, null))
        .thenReturn(mockResult);

    Siblings siblingInSearchResult = new Siblings();
    siblingInSearchResult.setPrimary(true);
    siblingInSearchResult.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(siblingInSearchResult);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetFiveUrn)));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetFourUrn)));

    Siblings dataset4Siblings = new Siblings();
    dataset4Siblings.setPrimary(true);
    dataset4Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    Siblings dataset5Siblings = new Siblings();
    dataset5Siblings.setPrimary(true);
    dataset5Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetTwoUrn)));

    Map<Urn, List<RecordTemplate>> siblingsMap =
        ImmutableMap.of(
            datasetOneUrn, ImmutableList.of(dataset1Siblings),
            datasetTwoUrn, ImmutableList.of(dataset2Siblings),
            datasetThreeUrn, ImmutableList.of(dataset3Siblings),
            datasetFourUrn, ImmutableList.of(dataset4Siblings),
            datasetFiveUrn, ImmutableList.of(dataset5Siblings));

    when(_mockEntityService.getLatestAspects(any(), any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage =
        service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert your lineage will not contain two siblings
    assertEquals(upstreamLineage, expectedResult);

    when(_graphService.getLineage(
            datasetThreeUrn, LineageDirection.UPSTREAM, 0, 100, 1, null, null))
        .thenReturn(siblingMockResult);

    when(_graphService.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 99, 1, null, null))
        .thenReturn(mockResult);

    siblingInSearchResult = new Siblings();
    siblingInSearchResult.setPrimary(false);
    siblingInSearchResult.setSiblings(new UrnArray(ImmutableList.of(datasetFourUrn)));

    when(_mockEntityService.getLatestAspect(datasetThreeUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(siblingInSearchResult);

    upstreamLineage = service.getLineage(datasetThreeUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    LineageRelationshipArray siblingExpectedRelationships = new LineageRelationshipArray();
    siblingExpectedRelationships.add(relationship1);
    siblingExpectedRelationships.add(relationship5);

    expectedResult.setRelationships(siblingExpectedRelationships);

    // assert your lineage will not contain two siblings
    assertEquals(upstreamLineage, expectedResult);
  }

  // we should be combining lineage of siblings of siblings
  // ie. dataset1 has sibling dataset2. dataset 2 has siblings dataset1 and dataset3. dataset3 has
  // sibling dataset2. dataset3 has upstream dataset4.
  // requesting upstream for dataset1 should give us dataset4
  @Test
  public void testUpstreamOfSiblingSiblings() {
    EntityLineageResult mockResult = new EntityLineageResult();
    EntityLineageResult expectedResult = new EntityLineageResult();

    LineageRelationshipArray relationships = new LineageRelationshipArray();
    LineageRelationshipArray expectedRelationships = new LineageRelationshipArray();

    LineageRelationship relationship = new LineageRelationship();
    relationship.setDegree(0);
    relationship.setType(downstreamOf);
    relationship.setEntity(datasetFourUrn);

    relationships.add(relationship);

    expectedRelationships.add(relationship);

    expectedResult.setCount(1);
    expectedResult.setStart(0);
    expectedResult.setTotal(1);
    expectedResult.setFiltered(0);
    expectedResult.setRelationships(expectedRelationships);

    mockResult.setStart(0);
    mockResult.setTotal(1);
    mockResult.setCount(1);
    mockResult.setRelationships(relationships);

    EntityLineageResult emptyLineageResult = new EntityLineageResult();
    emptyLineageResult.setRelationships(new LineageRelationshipArray());
    emptyLineageResult.setStart(0);
    emptyLineageResult.setTotal(0);
    emptyLineageResult.setCount(0);

    when(_graphService.getLineage(
            Mockito.eq(datasetOneUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .thenReturn(emptyLineageResult);

    when(_graphService.getLineage(
            Mockito.eq(datasetTwoUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .thenReturn(emptyLineageResult);

    when(_graphService.getLineage(
            Mockito.eq(datasetThreeUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .thenReturn(mockResult);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(true);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetTwoUrn)));

    when(_mockEntityService.getLatestAspect(datasetOneUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(dataset1Siblings);

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(true);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetOneUrn, datasetThreeUrn)));

    when(_mockEntityService.getLatestAspect(datasetTwoUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(dataset2Siblings);

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(true);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetTwoUrn)));

    when(_mockEntityService.getLatestAspect(datasetThreeUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(dataset3Siblings);

    Siblings dataset4Siblings = new Siblings();
    dataset4Siblings.setPrimary(true);
    dataset4Siblings.setSiblings(new UrnArray());

    when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(dataset4Siblings);

    Map<Urn, List<RecordTemplate>> siblingsMap =
        ImmutableMap.of(
            datasetOneUrn, ImmutableList.of(dataset1Siblings),
            datasetTwoUrn, ImmutableList.of(dataset2Siblings),
            datasetThreeUrn, ImmutableList.of(dataset3Siblings),
            datasetFourUrn, ImmutableList.of(dataset4Siblings));

    when(_mockEntityService.getLatestAspects(any(), any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    for (Urn urn : List.of(datasetOneUrn, datasetTwoUrn, datasetThreeUrn)) {
      EntityLineageResult upstreamLineage =
          service.getLineage(datasetOneUrn, LineageDirection.UPSTREAM, 0, 100, 1);

      assertEquals(upstreamLineage, expectedResult);
    }
  }

  @Test
  public void testRelationshipWithSibling() throws CloneNotSupportedException {
    EntityLineageResult mockResult = new EntityLineageResult();
    EntityLineageResult siblingMockResult = new EntityLineageResult();
    EntityLineageResult expectedResult = new EntityLineageResult();

    LineageRelationshipArray relationships = new LineageRelationshipArray();
    LineageRelationshipArray siblingRelationships = new LineageRelationshipArray();
    LineageRelationshipArray expectedRelationships = new LineageRelationshipArray();

    LineageRelationship relationship1 = new LineageRelationship();
    relationship1.setDegree(0);
    relationship1.setType(downstreamOf);
    relationship1.setEntity(datasetOneUrn);

    LineageRelationship relationship2 = new LineageRelationship();
    relationship2.setDegree(0);
    relationship2.setType(downstreamOf);
    relationship2.setEntity(datasetTwoUrn);

    LineageRelationship relationship3 = new LineageRelationship();
    relationship3.setDegree(0);
    relationship3.setType(downstreamOf);
    relationship3.setEntity(datasetThreeUrn);

    LineageRelationship relationship5 = new LineageRelationship();
    relationship5.setDegree(0);
    relationship5.setType(downstreamOf);
    relationship5.setEntity(datasetFiveUrn);

    relationships.add(relationship1);
    // relationship between entity and its sibling
    relationships.add(relationship3);

    expectedRelationships.add(relationship5);
    expectedRelationships.add(relationship1);

    expectedResult.setCount(2);
    expectedResult.setStart(0);
    expectedResult.setTotal(4);
    expectedResult.setFiltered(2);
    expectedResult.setRelationships(expectedRelationships);

    mockResult.setStart(0);
    mockResult.setTotal(2);
    mockResult.setCount(2);
    mockResult.setRelationships(relationships);

    siblingRelationships.add(relationship2);
    siblingRelationships.add(relationship5);
    siblingMockResult.setStart(0);
    siblingMockResult.setTotal(2);
    siblingMockResult.setCount(2);
    siblingMockResult.setRelationships(siblingRelationships);

    when(_graphService.getLineage(
            Mockito.eq(datasetThreeUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .then(invocation -> siblingMockResult.clone());

    when(_graphService.getLineage(
            Mockito.eq(datasetFourUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .then(invocation -> mockResult.clone());

    Siblings primarySibling = new Siblings();
    primarySibling.setPrimary(true);
    primarySibling.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(primarySibling);

    Siblings siblingInSearchResult = new Siblings();
    siblingInSearchResult.setPrimary(false);
    siblingInSearchResult.setSiblings(new UrnArray(ImmutableList.of(datasetFourUrn)));

    when(_mockEntityService.getLatestAspect(datasetThreeUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(siblingInSearchResult);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetFiveUrn)));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetFourUrn)));

    Siblings dataset4Siblings = new Siblings();
    dataset4Siblings.setPrimary(true);
    dataset4Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    Siblings dataset5Siblings = new Siblings();
    dataset5Siblings.setPrimary(true);
    dataset5Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetTwoUrn)));

    Map<Urn, List<RecordTemplate>> siblingsMap =
        ImmutableMap.of(
            datasetOneUrn, ImmutableList.of(dataset1Siblings),
            datasetTwoUrn, ImmutableList.of(dataset2Siblings),
            datasetThreeUrn, ImmutableList.of(dataset3Siblings),
            datasetFourUrn, ImmutableList.of(dataset4Siblings),
            datasetFiveUrn, ImmutableList.of(dataset5Siblings));

    when(_mockEntityService.getLatestAspects(any(), any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage =
        service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert your lineage will not contain two siblings
    assertEquals(upstreamLineage, expectedResult);

    // Now test for starting from the other sibling

    upstreamLineage = service.getLineage(datasetThreeUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    LineageRelationshipArray siblingExpectedRelationships = new LineageRelationshipArray();
    siblingExpectedRelationships.add(relationship1);
    siblingExpectedRelationships.add(relationship5);

    expectedResult.setRelationships(siblingExpectedRelationships);

    // assert your lineage will not contain two siblings
    assertEquals(upstreamLineage, expectedResult);
  }

  @Test
  public void testSiblingCombinations() throws URISyntaxException {
    Urn primarySiblingUrn =
        Urn.createFromString(
            "urn:li:" + datasetType + ":(urn:li:dataPlatform:dbt,PrimarySibling,PROD)");
    Urn alternateSiblingUrn =
        Urn.createFromString(
            "urn:li:" + datasetType + ":(urn:li:dataPlatform:snowflake,SecondarySibling,PROD)");

    Urn upstreamUrn1 =
        Urn.createFromString(
            "urn:li:" + datasetType + ":(urn:li:dataPlatform:snowflake,Upstream1,PROD)");
    Urn upstreamUrn2 =
        Urn.createFromString(
            "urn:li:" + datasetType + ":(urn:li:dataPlatform:snowflake,Upstream2,PROD)");

    LineageRelationshipArray alternateDownstreamRelationships = new LineageRelationshipArray();
    // Populate sibling service
    Siblings primarySiblings = new Siblings();
    primarySiblings.setPrimary(true);
    primarySiblings.setSiblings(new UrnArray(ImmutableList.of(alternateSiblingUrn)));

    when(_mockEntityService.getLatestAspect(primarySiblingUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(primarySiblings);

    Siblings secondarySiblings = new Siblings();
    secondarySiblings.setPrimary(false);
    secondarySiblings.setSiblings(new UrnArray(ImmutableList.of(primarySiblingUrn)));

    when(_mockEntityService.getLatestAspect(alternateSiblingUrn, SIBLINGS_ASPECT_NAME))
        .thenReturn(secondarySiblings);

    Map<Urn, List<RecordTemplate>> siblingsMap = new HashMap<>();
    siblingsMap.put(primarySiblingUrn, ImmutableList.of(primarySiblings));
    siblingsMap.put(alternateSiblingUrn, ImmutableList.of(secondarySiblings));

    // Create many downstreams of the alternate URN string
    final int numDownstreams = 42;
    for (int i = 0; i < numDownstreams; i++) {
      Urn downstreamUrn =
          Urn.createFromString(
              "urn:li:"
                  + datasetType
                  + ":(urn:li:dataPlatform:snowflake,Downstream"
                  + i
                  + ",PROD)");
      LineageRelationship relationship = new LineageRelationship();
      relationship.setDegree(0);
      relationship.setType(upstreamOf);
      relationship.setEntity(downstreamUrn);
      alternateDownstreamRelationships.add(relationship);
      siblingsMap.put(downstreamUrn, ImmutableList.of());
    }

    LineageRelationshipArray alternateUpstreamRelationships = new LineageRelationshipArray();
    for (Urn upstreamUrn : List.of(upstreamUrn1, upstreamUrn2, primarySiblingUrn)) {
      LineageRelationship relationship = new LineageRelationship();
      relationship.setDegree(0);
      relationship.setType(downstreamOf);
      relationship.setEntity(upstreamUrn);
      alternateUpstreamRelationships.add(relationship);
    }

    EntityLineageResult mockAlternateUpstreamResult = new EntityLineageResult();
    mockAlternateUpstreamResult.setRelationships(alternateUpstreamRelationships);
    mockAlternateUpstreamResult.setStart(0);
    mockAlternateUpstreamResult.setTotal(3);
    mockAlternateUpstreamResult.setCount(3);

    when(_graphService.getLineage(
            Mockito.eq(alternateSiblingUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .then(invocation -> mockAlternateUpstreamResult.clone());

    EntityLineageResult mockAlternateDownstreamResult = new EntityLineageResult();
    mockAlternateDownstreamResult.setRelationships(alternateDownstreamRelationships);
    mockAlternateDownstreamResult.setStart(0);
    mockAlternateDownstreamResult.setTotal(numDownstreams);
    mockAlternateDownstreamResult.setCount(numDownstreams);

    when(_graphService.getLineage(
            Mockito.eq(alternateSiblingUrn),
            Mockito.eq(LineageDirection.DOWNSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .then(invocation -> mockAlternateDownstreamResult.clone());

    // Set up mocks for primary sibling
    LineageRelationshipArray primaryUpstreamRelationships = new LineageRelationshipArray();
    for (Urn upstreamUrn : List.of(upstreamUrn1, upstreamUrn2)) {
      LineageRelationship relationship = new LineageRelationship();
      relationship.setDegree(0);
      relationship.setType(downstreamOf);
      relationship.setEntity(upstreamUrn);
      primaryUpstreamRelationships.add(relationship);
      siblingsMap.put(upstreamUrn, ImmutableList.of());
    }

    EntityLineageResult mockPrimaryUpstreamResult = new EntityLineageResult();
    mockPrimaryUpstreamResult.setRelationships(primaryUpstreamRelationships);
    mockPrimaryUpstreamResult.setStart(0);
    mockPrimaryUpstreamResult.setTotal(2);
    mockPrimaryUpstreamResult.setCount(2);

    when(_graphService.getLineage(
            Mockito.eq(primarySiblingUrn),
            Mockito.eq(LineageDirection.UPSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .then(invocation -> mockPrimaryUpstreamResult.clone());

    LineageRelationshipArray primaryDowntreamRelationships = new LineageRelationshipArray();
    LineageRelationship relationship = new LineageRelationship();
    relationship.setDegree(0);
    relationship.setType(upstreamOf);
    relationship.setEntity(alternateSiblingUrn);
    primaryDowntreamRelationships.add(relationship);

    EntityLineageResult mockPrimaryDownstreamResult = new EntityLineageResult();
    mockPrimaryDownstreamResult.setRelationships(primaryDowntreamRelationships);
    mockPrimaryDownstreamResult.setStart(0);
    mockPrimaryDownstreamResult.setTotal(1);
    mockPrimaryDownstreamResult.setCount(1);

    when(_graphService.getLineage(
            Mockito.eq(primarySiblingUrn),
            Mockito.eq(LineageDirection.DOWNSTREAM),
            Mockito.anyInt(),
            Mockito.anyInt(),
            Mockito.eq(1),
            Mockito.eq(null),
            Mockito.eq(null)))
        .then(invocation -> mockPrimaryDownstreamResult.clone());

    when(_mockEntityService.getLatestAspects(any(), any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    // Tests for separateSiblings = true: primary sibling
    EntityLineageResult primaryDownstreamSeparated =
        service.getLineage(
            primarySiblingUrn, LineageDirection.DOWNSTREAM, 0, 100, 1, true, Set.of(), null, null);

    LineageRelationshipArray expectedRelationships = new LineageRelationshipArray();
    expectedRelationships.add(relationship);

    EntityLineageResult expectedResultPrimarySeparated = new EntityLineageResult();
    expectedResultPrimarySeparated.setCount(1);
    expectedResultPrimarySeparated.setStart(0);
    expectedResultPrimarySeparated.setTotal(1);
    expectedResultPrimarySeparated.setFiltered(0);
    expectedResultPrimarySeparated.setRelationships(expectedRelationships);

    assertEquals(primaryDownstreamSeparated, expectedResultPrimarySeparated);

    EntityLineageResult primaryUpstreamSeparated =
        service.getLineage(
            primarySiblingUrn, LineageDirection.UPSTREAM, 0, 100, 1, true, Set.of(), null, null);
    EntityLineageResult expectedResultPrimaryUpstreamSeparated = new EntityLineageResult();
    expectedResultPrimaryUpstreamSeparated.setCount(2);
    expectedResultPrimaryUpstreamSeparated.setStart(0);
    expectedResultPrimaryUpstreamSeparated.setTotal(2);
    expectedResultPrimaryUpstreamSeparated.setFiltered(0);
    expectedResultPrimaryUpstreamSeparated.setRelationships(primaryUpstreamRelationships);

    assertEquals(primaryUpstreamSeparated, expectedResultPrimaryUpstreamSeparated);

    // Test for separateSiblings = true, secondary sibling
    EntityLineageResult secondarySiblingSeparated =
        service.getLineage(
            alternateSiblingUrn,
            LineageDirection.DOWNSTREAM,
            0,
            100,
            1,
            true,
            Set.of(),
            null,
            null);

    EntityLineageResult expectedResultSecondarySeparated = new EntityLineageResult();
    expectedResultSecondarySeparated.setCount(numDownstreams);
    expectedResultSecondarySeparated.setStart(0);
    expectedResultSecondarySeparated.setTotal(42);
    expectedResultSecondarySeparated.setFiltered(0);
    expectedResultSecondarySeparated.setRelationships(alternateDownstreamRelationships);

    assertEquals(secondarySiblingSeparated, expectedResultSecondarySeparated);

    EntityLineageResult secondaryUpstreamSeparated =
        service.getLineage(
            alternateSiblingUrn, LineageDirection.UPSTREAM, 0, 100, 1, true, Set.of(), null, null);
    EntityLineageResult expectedResultSecondaryUpstreamSeparated = new EntityLineageResult();
    expectedResultSecondaryUpstreamSeparated.setCount(3);
    expectedResultSecondaryUpstreamSeparated.setStart(0);
    expectedResultSecondaryUpstreamSeparated.setTotal(3);
    expectedResultSecondaryUpstreamSeparated.setFiltered(0);
    expectedResultSecondaryUpstreamSeparated.setRelationships(alternateUpstreamRelationships);

    assertEquals(secondaryUpstreamSeparated, expectedResultSecondaryUpstreamSeparated);

    // Test for separateSiblings = false, primary sibling
    EntityLineageResult primarySiblingNonSeparated =
        service.getLineage(
            primarySiblingUrn,
            LineageDirection.DOWNSTREAM,
            0,
            100,
            1,
            false,
            new HashSet<>(),
            null,
            null);
    EntityLineageResult expectedResultPrimaryNonSeparated = new EntityLineageResult();
    expectedResultPrimaryNonSeparated.setCount(numDownstreams);
    expectedResultPrimaryNonSeparated.setStart(0);
    expectedResultPrimaryNonSeparated.setTotal(43);
    expectedResultPrimaryNonSeparated.setFiltered(1);
    expectedResultPrimaryNonSeparated.setRelationships(alternateDownstreamRelationships);
    assertEquals(primarySiblingNonSeparated, expectedResultPrimaryNonSeparated);

    EntityLineageResult primarySiblingNonSeparatedUpstream =
        service.getLineage(
            primarySiblingUrn,
            LineageDirection.UPSTREAM,
            0,
            100,
            1,
            false,
            new HashSet<>(),
            null,
            null);
    EntityLineageResult expectedResultPrimaryUpstreamNonSeparated = new EntityLineageResult();
    expectedResultPrimaryUpstreamNonSeparated.setCount(2);
    expectedResultPrimaryUpstreamNonSeparated.setStart(0);
    expectedResultPrimaryUpstreamNonSeparated.setTotal(5);
    expectedResultPrimaryUpstreamNonSeparated.setFiltered(3);
    expectedResultPrimaryUpstreamNonSeparated.setRelationships(primaryUpstreamRelationships);
    assertEquals(primarySiblingNonSeparatedUpstream, expectedResultPrimaryUpstreamNonSeparated);

    // Test for separateSiblings = false, secondary sibling
    EntityLineageResult secondarySiblingNonSeparated =
        service.getLineage(
            alternateSiblingUrn,
            LineageDirection.DOWNSTREAM,
            0,
            100,
            1,
            false,
            new HashSet<>(),
            null,
            null);
    assertEquals(secondarySiblingNonSeparated, expectedResultPrimaryNonSeparated);

    EntityLineageResult secondarySiblingNonSeparatedUpstream =
        service.getLineage(
            alternateSiblingUrn,
            LineageDirection.UPSTREAM,
            0,
            100,
            1,
            false,
            new HashSet<>(),
            null,
            null);
    assertEquals(secondarySiblingNonSeparatedUpstream, expectedResultPrimaryUpstreamNonSeparated);
  }

  static Urn createFromString(@Nonnull String rawUrn) {
    try {
      return Urn.createFromString(rawUrn);
    } catch (URISyntaxException e) {
      return null;
    }
  }
}
