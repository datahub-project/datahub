package com.linkedin.metadata.config.search;

import static org.testng.Assert.assertEquals;

import java.util.Map;
import org.testng.annotations.Test;

public class BedrockConfigRegionTest {

  @Test
  public void explicitRegionIsKept() {
    EmbeddingProviderConfiguration.BedrockConfig config =
        new EmbeddingProviderConfiguration.BedrockConfig();

    config.setAwsRegion(" eu-west-1 ");

    assertEquals(config.getAwsRegion(), "eu-west-1");
  }

  @Test
  public void blankBoundValueUsesAwsRegion() {
    EmbeddingProviderConfiguration.BedrockConfig config =
        new EmbeddingProviderConfiguration.BedrockConfig();

    config.setAwsRegion(
        " ",
        Map.of(
                "BEDROCK_EMBEDDING_AWS_REGION",
                "",
                "BEDROCK_AWS_REGION",
                " ",
                "AWS_REGION",
                "eu-central-1")
            ::get);

    assertEquals(config.getAwsRegion(), "eu-central-1");
  }

  @Test
  public void blankBoundValueDefaultsWhenEveryNameIsBlank() {
    EmbeddingProviderConfiguration.BedrockConfig config =
        new EmbeddingProviderConfiguration.BedrockConfig();

    config.setAwsRegion(null, Map.of("BEDROCK_AWS_REGION", "", "AWS_REGION", " ")::get);

    assertEquals(config.getAwsRegion(), "us-west-2");
  }
}
