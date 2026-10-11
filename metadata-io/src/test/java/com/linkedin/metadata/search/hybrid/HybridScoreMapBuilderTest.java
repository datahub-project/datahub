package com.linkedin.metadata.search.hybrid;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class HybridScoreMapBuilderTest {

  private final HybridScoreMapBuilder builder =
      new HybridScoreMapBuilder(
          new HybridVectorScoreNormalizer(SearchEngineType.OPENSEARCH_3, "cosinesimil"));

  @Test
  public void testLexicalScoresPreserveRowOrderAndLastDuplicateScore() throws Exception {
    Urn first = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,first,PROD)");
    Urn second = Urn.createFromString("urn:li:chart:(dashboard,second)");

    Map<Urn, Double> scores =
        builder.lexicalScores(
            List.of(
                new SearchEntity().setEntity(first).setScore(2.0),
                new SearchEntity().setEntity(second).setScore(1.0),
                new SearchEntity().setEntity(first).setScore(3.0)));

    assertEquals(new ArrayList<>(scores.keySet()), List.of(first, second));
    assertEquals(scores.get(first), 3.0);
    assertEquals(scores.get(second), 1.0);
  }

  @Test
  public void testVectorScoresSkipsMalformedIdsAndKeepsBestDuplicateScore() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)");

    Map<Urn, Double> scores =
        builder.vectorScores(
            new KnnSearchResponse(
                List.of(
                    new KnnSearchResponse.Hit(urn.toString(), 0.4, Map.of()),
                    new KnnSearchResponse.Hit("not a urn", 1.0, Map.of()),
                    new KnnSearchResponse.Hit(urn.toString(), 0.7, Map.of()),
                    new KnnSearchResponse.Hit(urn.toString(), 0.2, Map.of()))));

    // OpenSearch cosine is already scaled to [0, 1]; the normalizer keeps it direct (no halving),
    // so the best duplicate 0.7 stays 0.7. See HybridVectorScoreNormalizer.
    assertEquals(scores, Map.of(urn, 0.7));
  }

  @Test
  public void testVectorScoresSkipsUndecodableHitId() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)");

    Map<Urn, Double> scores =
        builder.vectorScores(
            new KnnSearchResponse(
                List.of(
                    new KnnSearchResponse.Hit("%ZZ", 0.9, Map.of()),
                    new KnnSearchResponse.Hit(urn.toString(), 0.4, Map.of()))));

    assertEquals(scores, Map.of(urn, 0.4));
  }

  @Test
  public void testVectorScoresUsesSourceUrnWhenHitIdIsEncoded() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)");

    Map<Urn, Double> scores =
        builder.vectorScores(
            new KnnSearchResponse(
                List.of(
                    new KnnSearchResponse.Hit(
                        "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Ctable%2CPROD%29",
                        0.8, Map.of("urn", urn.toString())))));

    assertEquals(scores, Map.of(urn, 0.8));
  }

  @Test
  public void testVectorScoresDecodesEncodedHitIdWhenSourceUrnMissing() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)");

    Map<Urn, Double> scores =
        builder.vectorScores(
            new KnnSearchResponse(
                List.of(
                    new KnnSearchResponse.Hit(
                        "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Ctable%2CPROD%29",
                        0.8, Map.of()))));

    assertEquals(scores, Map.of(urn, 0.8));
  }
}
