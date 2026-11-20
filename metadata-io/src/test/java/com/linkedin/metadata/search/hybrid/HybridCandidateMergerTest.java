package com.linkedin.metadata.search.hybrid;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class HybridCandidateMergerTest {

  private Urn datasetA;
  private Urn datasetB;
  private Urn datasetC;

  @BeforeClass
  public void setUp() throws URISyntaxException {
    datasetA = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,a,PROD)");
    datasetB = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,b,PROD)");
    datasetC = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,c,PROD)");
  }

  @Test
  public void testMergeCombinesAndSortsLexicalBackedCandidates() {
    HybridCandidateMerger merger =
        new HybridCandidateMerger(
            new HybridLexicalScoreNormalizer(0d, 100d, 4d), new HybridScoreCombiner(0.5d));
    Map<Urn, Double> lexicalScores = new LinkedHashMap<>();
    lexicalScores.put(datasetA, 100d);
    lexicalScores.put(datasetB, 0d);
    Map<Urn, Double> vectorScores = Map.of(datasetA, 0.2d, datasetB, 0.2d, datasetC, 1d);

    List<HybridCandidate> candidates = merger.merge(lexicalScores, vectorScores);

    assertEquals(candidates.size(), 2);
    assertEquals(candidates.get(0).entity(), datasetA);
    assertEquals(candidates.get(1).entity(), datasetB);
    assertTrue(
        candidates.get(0).combinedScore() > candidates.get(1).combinedScore(),
        "lexical and vector scores should be score-combined before ordering");
  }

  @Test
  public void testMergeExcludesVectorOnlyCandidates() {
    HybridCandidateMerger merger =
        new HybridCandidateMerger(
            new HybridLexicalScoreNormalizer(0d, 100d, 4d), new HybridScoreCombiner(0.8d));

    List<HybridCandidate> candidates =
        merger.merge(Map.of(datasetA, 20d), Map.of(datasetA, 0.1d, datasetB, 1d));

    assertEquals(candidates.size(), 1);
    assertEquals(candidates.get(0).entity(), datasetA);
  }

  @Test
  public void testMergeTreatsMissingVectorScoreAsLowContribution() {
    HybridCandidateMerger merger =
        new HybridCandidateMerger(
            new HybridLexicalScoreNormalizer(0d, 100d, 4d), new HybridScoreCombiner(0.5d));

    List<HybridCandidate> candidates = merger.merge(Map.of(datasetA, 50d), null);

    assertEquals(candidates.size(), 1);
    assertEquals(candidates.get(0).entity(), datasetA);
    assertEquals(candidates.get(0).vectorScore(), HybridCandidateMerger.ABSENT_VECTOR_SCORE);
    assertEquals(candidates.get(0).normalizedVectorScore(), 0d);
  }

  @Test
  public void testMergeRanksWeakVectorHitAboveAbsentVectorWhenLexicalTies() {
    HybridCandidateMerger merger =
        new HybridCandidateMerger(
            new HybridLexicalScoreNormalizer(0d, 100d, 4d), new HybridScoreCombiner(0.5d));
    Map<Urn, Double> lexicalScores = new LinkedHashMap<>();
    lexicalScores.put(datasetA, 50d);
    lexicalScores.put(datasetB, 50d);

    List<HybridCandidate> candidates = merger.merge(lexicalScores, Map.of(datasetB, 0.2d));

    assertEquals(candidates.size(), 2);
    assertEquals(candidates.get(0).entity(), datasetB);
    assertEquals(candidates.get(1).entity(), datasetA);
  }
}
