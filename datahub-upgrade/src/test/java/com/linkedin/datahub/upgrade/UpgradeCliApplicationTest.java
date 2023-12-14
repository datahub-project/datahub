package com.linkedin.datahub.upgrade;

import static org.testng.AssertJUnit.*;

import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class})
public class UpgradeCliApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("restoreIndices")
  private RestoreIndices restoreIndices;

  @Autowired
  @Named("buildIndices")
  private BuildIndices buildIndices;

  @Autowired private ESIndexBuilder esIndexBuilder;

  @Test
  public void testRestoreIndicesInit() {
    /*
     This might seem like a simple test however it does exercise the spring autowiring of the kafka health check bean
    */
    assertTrue(restoreIndices.steps().size() >= 3);
  }

  @Test
  public void testBuildIndicesInit() {
    assertEquals("BuildIndices", buildIndices.id());
    assertTrue(buildIndices.steps().size() >= 3);
    assertNotNull(esIndexBuilder.getElasticSearchConfiguration());
    assertNotNull(esIndexBuilder.getElasticSearchConfiguration().getBuildIndices());
    assertTrue(esIndexBuilder.getElasticSearchConfiguration().getBuildIndices().isCloneIndices());
    assertFalse(
        esIndexBuilder.getElasticSearchConfiguration().getBuildIndices().isAllowDocCountMismatch());
  }
}
