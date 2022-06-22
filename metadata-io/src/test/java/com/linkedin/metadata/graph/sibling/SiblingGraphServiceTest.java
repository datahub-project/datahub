package com.linkedin.metadata.graph.sibling;

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
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;


public class SiblingGraphServiceTest {

  /**
   * Some test URN types.
   */
  protected static String datasetType = "dataset";
  protected static String userType = "user";

  /**
   * Some test datasets.
   */
  protected static String datasetOneUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetOne,PROD)";
  protected static String datasetTwoUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetTwo,PROD)";
  protected static String datasetThreeUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetThree,PROD)";
  protected static String datasetFourUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetFour,PROD)";
  protected static String datasetFiveUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetFive,PROD)";

  protected static Urn datasetOneUrn = createFromString(datasetOneUrnString);
  protected static Urn datasetTwoUrn = createFromString(datasetTwoUrnString);
  protected static Urn datasetThreeUrn = createFromString(datasetThreeUrnString);
  protected static Urn datasetFourUrn = createFromString(datasetFourUrnString);
  protected static Urn datasetFiveUrn = createFromString(datasetFiveUrnString);


  /**
   * Some test relationships.
   */
  protected static String downstreamOf = "DownstreamOf";

  private GraphService _graphService;
  private SiblingGraphService _client;
  EntityService _mockEntityService;

  @BeforeClass
  public void setup() {
    _mockEntityService = Mockito.mock(EntityService.class);
    _graphService = Mockito.mock(GraphService.class);
    _client = new SiblingGraphService(_mockEntityService, _graphService);
  }

  @Test
  public void testNoSiblingMetadata() throws Exception {
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
    mockResult.setRelationships(relationships);

    Mockito.when(_graphService.getLineage(
        datasetFourUrn,  LineageDirection.UPSTREAM, 0, 100, 1
    )).thenReturn(mockResult);

    Mockito.when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME)).thenReturn(null);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage = service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert sibling graph service is a pass through in the case that there is no sibling metadataa
    assertEquals(upstreamLineage, mockResult);
  }

  @Test
  public void testNoSiblingInResults() throws Exception {
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
    mockResult.setRelationships(relationships);

    Mockito.when(_graphService.getLineage(
        datasetFourUrn,  LineageDirection.UPSTREAM, 0, 100, 1
    )).thenReturn(mockResult);

    siblingMockResult.setStart(0);
    siblingMockResult.setTotal(0);
    siblingMockResult.setCount(0);
    siblingMockResult.setRelationships(new LineageRelationshipArray());

    Mockito.when(_graphService.getLineage(
        datasetFiveUrn,  LineageDirection.UPSTREAM, 0, 97, 1
    )).thenReturn(siblingMockResult);

    Siblings noRelevantSiblingsResponse = new Siblings();
    noRelevantSiblingsResponse.setPrimary(true);
    noRelevantSiblingsResponse.setSiblings(new UrnArray(ImmutableList.of(datasetFiveUrn)));

    Mockito.when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME)).thenReturn(noRelevantSiblingsResponse);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Map<Urn, List<RecordTemplate>> siblingsMap = ImmutableMap.of(
        datasetOneUrn, ImmutableList.of(dataset1Siblings),
        datasetTwoUrn, ImmutableList.of(dataset2Siblings),
        datasetThreeUrn, ImmutableList.of(dataset3Siblings)
    );

    Mockito.when(_mockEntityService.getLatestAspects(Mockito.any(), Mockito.any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage = service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

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

    Mockito.when(_graphService.getLineage(
        datasetThreeUrn,  LineageDirection.UPSTREAM, 0, 98, 1
    )).thenReturn(siblingMockResult);


    Mockito.when(_graphService.getLineage(
        datasetFourUrn,  LineageDirection.UPSTREAM, 0, 100, 1
    )).thenReturn(mockResult);

    Siblings siblingInSearchResult = new Siblings();
    siblingInSearchResult.setPrimary(true);
    siblingInSearchResult.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    Mockito.when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME)).thenReturn(siblingInSearchResult);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Map<Urn, List<RecordTemplate>> siblingsMap = ImmutableMap.of(
        datasetOneUrn, ImmutableList.of(dataset1Siblings),
        datasetTwoUrn, ImmutableList.of(dataset2Siblings),
        datasetThreeUrn, ImmutableList.of(dataset3Siblings)
    );

    Mockito.when(_mockEntityService.getLatestAspects(Mockito.any(), Mockito.any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult expectedResult = mockResult.clone();
    expectedResult.setTotal(3);
    expectedResult.setCount(2);
    expectedResult.setRelationships(new LineageRelationshipArray(relationship1, relationship2));

    EntityLineageResult upstreamLineage = service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert your sibling will be filtered out of your lineage
    assertEquals(upstreamLineage, expectedResult);
    assertEquals(upstreamLineage.getRelationships().size(), 2);
  }

  @Test
  public void testCombineSiblingResult() throws Exception {
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
    expectedRelationships.add(relationship1);

    expectedResult.setCount(3);
    expectedResult.setStart(0);
    expectedResult.setTotal(3);
    expectedResult.setRelationships(expectedRelationships);

    mockResult.setStart(0);
    mockResult.setTotal(1);
    mockResult.setCount(1);
    mockResult.setRelationships(relationships);

    siblingRelationships.add(relationship2);
    siblingRelationships.add(relationship4);
    siblingMockResult.setStart(0);
    siblingMockResult.setTotal(2);
    siblingMockResult.setCount(2);
    siblingMockResult.setRelationships(siblingRelationships);

    Mockito.when(_graphService.getLineage(
        datasetThreeUrn,  LineageDirection.UPSTREAM, 0, 99, 1
    )).thenReturn(siblingMockResult);


    Mockito.when(_graphService.getLineage(
        datasetFourUrn,  LineageDirection.UPSTREAM, 0, 100, 1
    )).thenReturn(mockResult);

    Siblings siblingInSearchResult = new Siblings();
    siblingInSearchResult.setPrimary(true);
    siblingInSearchResult.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    Mockito.when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME)).thenReturn(siblingInSearchResult);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Map<Urn, List<RecordTemplate>> siblingsMap = ImmutableMap.of(
        datasetOneUrn, ImmutableList.of(dataset1Siblings),
        datasetTwoUrn, ImmutableList.of(dataset2Siblings),
        datasetThreeUrn, ImmutableList.of(dataset3Siblings),
        datasetFiveUrn, ImmutableList.of(dataset3Siblings)
    );

    Mockito.when(_mockEntityService.getLatestAspects(Mockito.any(), Mockito.any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage = service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert your lineage will be combined with your siblings lineage
    assertEquals(upstreamLineage, expectedResult);
  }

  @Test
  public void testUpstreamOfSiblings() throws Exception {
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

    expectedRelationships.add(relationship4);
    expectedRelationships.add(relationship1);

    expectedResult.setCount(2);
    expectedResult.setStart(0);
    expectedResult.setTotal(3);
    expectedResult.setRelationships(expectedRelationships);

    mockResult.setStart(0);
    mockResult.setTotal(1);
    mockResult.setCount(1);
    mockResult.setRelationships(relationships);

    siblingRelationships.add(relationship2);
    siblingRelationships.add(relationship4);
    siblingMockResult.setStart(0);
    siblingMockResult.setTotal(2);
    siblingMockResult.setCount(2);
    siblingMockResult.setRelationships(siblingRelationships);

    Mockito.when(_graphService.getLineage(
        datasetThreeUrn,  LineageDirection.UPSTREAM, 0, 99, 1
    )).thenReturn(siblingMockResult);


    Mockito.when(_graphService.getLineage(
        datasetFourUrn,  LineageDirection.UPSTREAM, 0, 100, 1
    )).thenReturn(mockResult);

    Siblings siblingInSearchResult = new Siblings();
    siblingInSearchResult.setPrimary(true);
    siblingInSearchResult.setSiblings(new UrnArray(ImmutableList.of(datasetThreeUrn)));

    Mockito.when(_mockEntityService.getLatestAspect(datasetFourUrn, SIBLINGS_ASPECT_NAME)).thenReturn(siblingInSearchResult);

    Siblings dataset1Siblings = new Siblings();
    dataset1Siblings.setPrimary(false);
    dataset1Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset2Siblings = new Siblings();
    dataset2Siblings.setPrimary(false);
    dataset2Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetFiveUrn)));

    Siblings dataset3Siblings = new Siblings();
    dataset3Siblings.setPrimary(false);
    dataset3Siblings.setSiblings(new UrnArray(ImmutableList.of()));

    Siblings dataset5Siblings = new Siblings();
    dataset5Siblings.setPrimary(true);
    dataset5Siblings.setSiblings(new UrnArray(ImmutableList.of(datasetTwoUrn)));

    Map<Urn, List<RecordTemplate>> siblingsMap = ImmutableMap.of(
        datasetOneUrn, ImmutableList.of(dataset1Siblings),
        datasetTwoUrn, ImmutableList.of(dataset2Siblings),
        datasetThreeUrn, ImmutableList.of(dataset3Siblings),
        datasetFiveUrn, ImmutableList.of(dataset5Siblings)
    );

    Mockito.when(_mockEntityService.getLatestAspects(Mockito.any(), Mockito.any())).thenReturn(siblingsMap);

    SiblingGraphService service = _client;

    EntityLineageResult upstreamLineage = service.getLineage(datasetFourUrn, LineageDirection.UPSTREAM, 0, 100, 1);

    // assert your lineage will not contain two siblings
    assertEquals(upstreamLineage, expectedResult);
  }

  static Urn createFromString(@Nonnull String rawUrn) {
    try {
      return Urn.createFromString(rawUrn);
    } catch (URISyntaxException e) {
      return null;
    }
  }
}
