/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
