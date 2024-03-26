package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.Embed;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EmbedMapperTest {
  @Test
  public void testEmbedMapper() throws Exception {
    final String renderUrl = "https://www.google.com";
    final Embed result =
        EmbedMapper.map(null, new com.linkedin.common.Embed().setRenderUrl(renderUrl));
    Assert.assertEquals(result.getRenderUrl(), renderUrl);
  }
}
