package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.VersionedDataset;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VersionedDatasetMapperTest {

  private static final Urn TEST_DATASET_URN =
      Urn.createFromTuple(Constants.DATASET_ENTITY_NAME, "test");

  @Test
  public void testVersionedDatasetMapperViewPropertiesWithFormattedLogic() {
    final com.linkedin.dataset.ViewProperties input = new com.linkedin.dataset.ViewProperties();
    input.setMaterialized(true);
    input.setViewLanguage("SQL");
    input.setViewLogic("select * from {{ ref('upstream') }}");
    input.setFormattedViewLogic("select * from warehouse.schema.upstream");

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.VIEW_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final VersionedDataset actual = VersionedDatasetMapper.map(null, response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertTrue(actual.getViewProperties().getMaterialized());
    Assert.assertEquals(actual.getViewProperties().getLanguage(), "SQL");
    Assert.assertEquals(
        actual.getViewProperties().getLogic(), "select * from {{ ref('upstream') }}");
    Assert.assertEquals(
        actual.getViewProperties().getFormattedLogic(), "select * from warehouse.schema.upstream");
  }

  @Test
  public void testVersionedDatasetMapperViewPropertiesWithoutFormattedLogic() {
    final com.linkedin.dataset.ViewProperties input = new com.linkedin.dataset.ViewProperties();
    input.setMaterialized(false);
    input.setViewLanguage("SQL");
    input.setViewLogic("select 1");

    final Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.VIEW_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(input.data())));
    final EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(TEST_DATASET_URN)
            .setAspects(new EnvelopedAspectMap(aspects));

    final VersionedDataset actual = VersionedDatasetMapper.map(null, response);

    Assert.assertNotNull(actual.getViewProperties());
    Assert.assertEquals(actual.getViewProperties().getLogic(), "select 1");
    Assert.assertNull(actual.getViewProperties().getFormattedLogic());
  }
}
