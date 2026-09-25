package com.linkedin.metadata.queue;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

public class PriorityBandConfigTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String THREE_BAND_JSON =
      "[{\"range\":[0,3],\"weight\":70},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]";

  @Test
  public void parseValidThreeBand() {
    PriorityBandConfig config = PriorityBandConfig.parse(MAPPER, THREE_BAND_JSON);
    assertEquals(config.bands().size(), 3);
    assertEquals(config.bands().get(0), new PriorityBand(0, 3, 70));
    assertEquals(config.bands().get(1), new PriorityBand(4, 6, 20));
    assertEquals(config.bands().get(2), new PriorityBand(7, 9, 10));
  }

  @Test
  public void parseSingleBandCoveringFullRange() {
    PriorityBandConfig config =
        PriorityBandConfig.parse(MAPPER, "[{\"range\":[0,9],\"weight\":1}]");
    assertEquals(config.bands().size(), 1);
    assertEquals(config.bands().get(0), new PriorityBand(0, 9, 1));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void rejectOverlappingBands() {
    PriorityBandConfig.parse(
        MAPPER, "[{\"range\":[0,5],\"weight\":50},{\"range\":[4,9],\"weight\":50}]");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void rejectGapInCoverage() {
    PriorityBandConfig.parse(
        MAPPER, "[{\"range\":[0,3],\"weight\":50},{\"range\":[5,9],\"weight\":50}]");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void rejectBandNotCoveringFullRange() {
    PriorityBandConfig.parse(MAPPER, "[{\"range\":[0,8],\"weight\":100}]");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void rejectZeroWeight() {
    PriorityBandConfig.parse(
        MAPPER,
        "[{\"range\":[0,3],\"weight\":0},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void rejectNegativeWeight() {
    PriorityBandConfig.parse(
        MAPPER,
        "[{\"range\":[0,3],\"weight\":-1},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void rejectOutOfRangePriority() {
    PriorityBandConfig.parse(MAPPER, "[{\"range\":[0,10],\"weight\":100}]");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void rejectEmptyBands() {
    PriorityBandConfig.parse(MAPPER, "[]");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void rejectInvalidJson() {
    PriorityBandConfig.parse(MAPPER, "not json");
  }

  @Test
  public void batchLimitsDistributesProportionally() {
    PriorityBandConfig config = PriorityBandConfig.parse(MAPPER, THREE_BAND_JSON);
    int[] limits = config.batchLimits(10);
    assertEquals(limits[0], 7);
    assertEquals(limits[1], 2);
    assertEquals(limits[2], 1);
  }

  @Test
  public void batchLimitsDistributesRemainder() {
    PriorityBandConfig config = PriorityBandConfig.parse(MAPPER, THREE_BAND_JSON);
    int[] limits = config.batchLimits(100);
    assertEquals(limits[0] + limits[1] + limits[2], 100);
    assertEquals(limits[0], 70);
    assertEquals(limits[1], 20);
    assertEquals(limits[2], 10);
  }

  @Test
  public void batchLimitsHandlesSmallBatch() {
    PriorityBandConfig config = PriorityBandConfig.parse(MAPPER, THREE_BAND_JSON);
    int[] limits = config.batchLimits(3);
    assertEquals(limits[0] + limits[1] + limits[2], 3);
    assertTrue(limits[0] >= 1);
  }

  @Test
  public void batchLimitsZeroReturnZeros() {
    PriorityBandConfig config = PriorityBandConfig.parse(MAPPER, THREE_BAND_JSON);
    int[] limits = config.batchLimits(0);
    assertEquals(limits[0], 0);
    assertEquals(limits[1], 0);
    assertEquals(limits[2], 0);
  }

  @Test
  public void batchLimitsEqualWeights() {
    PriorityBandConfig config =
        PriorityBandConfig.parse(
            MAPPER,
            "[{\"range\":[0,3],\"weight\":1},{\"range\":[4,6],\"weight\":1},{\"range\":[7,9],\"weight\":1}]");
    int[] limits = config.batchLimits(9);
    assertEquals(limits[0], 3);
    assertEquals(limits[1], 3);
    assertEquals(limits[2], 3);
  }
}
