package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.MaeConsumerConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Unit tests for MAE / global RestClient timeout merge in {@link SearchClientShimFactory}. */
public class SearchClientShimFactoryMaeTimeoutMergeTest {

  @Test
  public void socketWhenMaeNull_usesElasticsearch() {
    assertEquals(SearchClientShimFactory.mergeRestClientSocketTimeoutMs(30_000, null), 30_000);
  }

  @Test
  public void socketWhenMaeDisabled_usesElasticsearch() {
    MaeConsumerConfiguration mae = new MaeConsumerConfiguration();
    mae.setEnabled(false);
    mae.setElasticsearch(mel(60_000, 10_000));
    assertEquals(SearchClientShimFactory.mergeRestClientSocketTimeoutMs(30_000, mae), 30_000);
  }

  @Test
  public void socketWhenMaeEnabledButNestedElasticsearchNull_usesElasticsearch() {
    MaeConsumerConfiguration mae = new MaeConsumerConfiguration();
    mae.setEnabled(true);
    mae.setElasticsearch(null);
    assertEquals(SearchClientShimFactory.mergeRestClientSocketTimeoutMs(30_000, mae), 30_000);
  }

  @Test
  public void socketWhenOverrideHigher_takesMax() {
    MaeConsumerConfiguration mae = maeEnabled(mel(90_000, null));
    assertEquals(SearchClientShimFactory.mergeRestClientSocketTimeoutMs(30_000, mae), 90_000);
  }

  @Test
  public void socketWhenOverrideLower_keepsElasticsearch() {
    MaeConsumerConfiguration mae = maeEnabled(mel(10_000, null));
    assertEquals(SearchClientShimFactory.mergeRestClientSocketTimeoutMs(30_000, mae), 30_000);
  }

  @Test
  public void socketWhenOverrideNull_usesElasticsearch() {
    MaeConsumerConfiguration.Elasticsearch mel = new MaeConsumerConfiguration.Elasticsearch();
    mel.setSocketTimeoutMs(null);
    mel.setConnectionRequestTimeoutMs(5_000);
    assertEquals(
        SearchClientShimFactory.mergeRestClientSocketTimeoutMs(30_000, maeEnabled(mel)), 30_000);
  }

  @Test
  public void socketWhenOverrideNegative_ignored() {
    // -1 defers to global per maeConsumer docs; non-negative values participate in Math.max
    MaeConsumerConfiguration mae = maeEnabled(mel(-1, 5_000));
    assertEquals(SearchClientShimFactory.mergeRestClientSocketTimeoutMs(30_000, mae), 30_000);
  }

  @Test
  public void connectionWhenMaeNull_usesElasticsearch() {
    assertEquals(
        SearchClientShimFactory.mergeRestClientConnectionRequestTimeoutMs(5_000, null), 5_000);
  }

  @Test
  public void connectionOverrideHigher_takesMax() {
    MaeConsumerConfiguration mae = maeEnabled(mel(null, 20_000));
    assertEquals(
        SearchClientShimFactory.mergeRestClientConnectionRequestTimeoutMs(5_000, mae), 20_000);
  }

  @DataProvider
  public Object[][] enabledNotTrueCases() {
    return new Object[][] {
      {null}, {Boolean.FALSE},
    };
  }

  @Test(dataProvider = "enabledNotTrueCases")
  public void socketWhenMaeNotExplicitlyEnabled_noMerge(Boolean enabled) {
    MaeConsumerConfiguration mae = new MaeConsumerConfiguration();
    mae.setEnabled(enabled);
    mae.setElasticsearch(mel(999_000, 999_000));
    assertEquals(SearchClientShimFactory.mergeRestClientSocketTimeoutMs(30_000, mae), 30_000);
  }

  private static MaeConsumerConfiguration.Elasticsearch mel(
      Integer socketTimeoutMs, Integer connectionRequestTimeoutMs) {
    MaeConsumerConfiguration.Elasticsearch mel = new MaeConsumerConfiguration.Elasticsearch();
    mel.setSocketTimeoutMs(socketTimeoutMs);
    mel.setConnectionRequestTimeoutMs(connectionRequestTimeoutMs);
    return mel;
  }

  private static MaeConsumerConfiguration maeEnabled(MaeConsumerConfiguration.Elasticsearch mel) {
    MaeConsumerConfiguration mae = new MaeConsumerConfiguration();
    mae.setEnabled(true);
    mae.setElasticsearch(mel);
    return mae;
  }
}
