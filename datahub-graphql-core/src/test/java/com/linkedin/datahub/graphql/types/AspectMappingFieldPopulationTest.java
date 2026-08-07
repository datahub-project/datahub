package com.linkedin.datahub.graphql.types;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.types.chart.mappers.ChartMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetMapper;
import com.linkedin.datahub.graphql.types.domain.DomainMapper;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.key.DomainKey;
import org.testng.annotations.Test;

/**
 * Proves that when the optimizer fetches ONLY the aspects a field's {@code @aspectMapping}
 * declares, the corresponding GraphQL field still populates via the real mapper. This guards
 * against a wrong or incomplete aspect list silently returning null fields (which the
 * missing-directive fallback does NOT protect against).
 */
public class AspectMappingFieldPopulationTest {

  private static EnvelopedAspect env(RecordTemplate aspect) {
    return new EnvelopedAspect().setValue(new Aspect(aspect.data()));
  }

  @Test
  public void testDatasetNameAndPropertiesPopulateFromMappedAspects() throws Exception {
    Urn urn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
    DatasetKey key =
        new DatasetKey()
            .setPlatform(Urn.createFromTuple("dataPlatform", "mysql"))
            .setName("my_db.my_table")
            .setOrigin(FabricType.PROD);
    DatasetProperties props = new DatasetProperties().setName("My Table").setDescription("desc");

    // Only the aspects mapped for `name`/`properties`: datasetKey + datasetProperties.
    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DATASET_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.DATASET_KEY_ASPECT_NAME, env(key),
                        Constants.DATASET_PROPERTIES_ASPECT_NAME, env(props))));

    Dataset dataset = DatasetMapper.map(null, response);

    assertNotNull(dataset);
    assertEquals(dataset.getUrn(), urn.toString());
    assertNotNull(dataset.getProperties(), "properties must populate from datasetProperties");
    assertEquals(dataset.getProperties().getName(), "My Table");
    assertNotNull(dataset.getName(), "name must populate from mapped aspects");
  }

  @Test
  public void testChartPropertiesPopulateFromChartInfo() throws Exception {
    Urn urn = Urn.createFromString("urn:li:chart:(looker,my_chart)");
    ChartKey key = new ChartKey().setChartId("my_chart").setDashboardTool("looker");
    AuditStamp stamp =
        new AuditStamp().setTime(0L).setActor(Urn.createFromString("urn:li:corpuser:test"));
    ChartInfo info =
        new ChartInfo()
            .setTitle("My Chart")
            .setDescription("chart desc")
            .setLastModified(new ChangeAuditStamps().setCreated(stamp).setLastModified(stamp));

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.CHART_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.CHART_KEY_ASPECT_NAME, env(key),
                        Constants.CHART_INFO_ASPECT_NAME, env(info))));

    Chart chart = ChartMapper.map(null, response);

    assertNotNull(chart);
    assertEquals(chart.getUrn(), urn.toString());
    assertNotNull(chart.getProperties(), "properties must populate from chartInfo");
    assertEquals(chart.getProperties().getName(), "My Chart");
  }

  @Test
  public void testDomainPropertiesPopulateFromDomainProperties() throws Exception {
    Urn urn = Urn.createFromString("urn:li:domain:my-domain");
    DomainProperties props = new DomainProperties().setName("My Domain").setDescription("dom desc");

    EntityResponse response =
        new EntityResponse()
            .setEntityName(Constants.DOMAIN_ENTITY_NAME)
            .setUrn(urn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.DOMAIN_KEY_ASPECT_NAME, env(new DomainKey().setId("my-domain")),
                        Constants.DOMAIN_PROPERTIES_ASPECT_NAME, env(props))));

    Domain domain = DomainMapper.map(null, response);

    assertNotNull(domain);
    assertEquals(domain.getUrn(), urn.toString());
    assertNotNull(domain.getProperties(), "properties must populate from domainProperties");
    assertEquals(domain.getProperties().getName(), "My Domain");
  }
}
