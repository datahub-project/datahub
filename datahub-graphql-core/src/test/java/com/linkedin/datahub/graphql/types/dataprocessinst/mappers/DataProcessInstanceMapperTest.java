package com.linkedin.datahub.graphql.types.dataprocessinst.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.Container;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.ml.metadata.MLTrainingRunProperties;
import java.util.HashMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProcessInstanceMapperTest {

  private static final String TEST_PLATFORM_URN = "urn:li:dataPlatform:kafka";
  private static final String TEST_INSTANCE_URN =
      "urn:li:dataProcessInstance:(test-workflow,test-instance)";
  private static final String TEST_CONTAINER_URN = "urn:li:container:testContainer";
  private static final String TEST_EXTERNAL_URL = "https://example.com/process";
  private static final String TEST_NAME = "Test Process Instance";

  private EntityResponse entityResponse;
  private Urn urn;

  @BeforeMethod
  public void setup() throws Exception {
    urn = Urn.createFromString(TEST_INSTANCE_URN);
    entityResponse = new EntityResponse();
    entityResponse.setUrn(urn);
    entityResponse.setAspects(new EnvelopedAspectMap(new HashMap<>()));
  }

  @Test
  public void testMapBasicFields() throws Exception {
    DataProcessInstance instance = DataProcessInstanceMapper.map(null, entityResponse);

    assertNotNull(instance);
    assertEquals(instance.getUrn(), urn.toString());
    assertEquals(instance.getType(), EntityType.DATA_PROCESS_INSTANCE);
  }

  @Test
  public void testMapDataProcessProperties() throws Exception {
    // Create DataProcessInstanceProperties
    DataProcessInstanceProperties properties = new DataProcessInstanceProperties();
    properties.setName(TEST_NAME);
    properties.setExternalUrl(new Url(TEST_EXTERNAL_URL));

    // Add properties aspect
    addAspect(Constants.DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME, properties);

    DataProcessInstance instance = DataProcessInstanceMapper.map(null, entityResponse);

    assertNotNull(instance.getProperties());
    assertEquals(instance.getName(), TEST_NAME);
    assertEquals(instance.getExternalUrl(), TEST_EXTERNAL_URL);
  }

  @Test
  public void testMapPlatformInstance() throws Exception {
    // Create DataPlatformInstance
    DataPlatformInstance platformInstance = new DataPlatformInstance();
    platformInstance.setPlatform(Urn.createFromString(TEST_PLATFORM_URN));

    // Add platform instance aspect
    addAspect(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME, platformInstance);

    DataProcessInstance instance = DataProcessInstanceMapper.map(null, entityResponse);

    assertNotNull(instance.getDataPlatformInstance());
    assertNotNull(instance.getDataPlatformInstance().getPlatform());
    assertEquals(instance.getDataPlatformInstance().getPlatform().getUrn(), TEST_PLATFORM_URN);
    assertEquals(
        instance.getDataPlatformInstance().getPlatform().getType(), EntityType.DATA_PLATFORM);
  }

  @Test
  public void testMapContainer() throws Exception {
    // Create Container aspect
    Container container = new Container();
    container.setContainer(Urn.createFromString(TEST_CONTAINER_URN));

    // Add container aspect
    addAspect(Constants.CONTAINER_ASPECT_NAME, container);

    DataProcessInstance instance = DataProcessInstanceMapper.map(null, entityResponse);

    assertNotNull(instance.getContainer());
    assertEquals(instance.getContainer().getUrn(), TEST_CONTAINER_URN);
    assertEquals(instance.getContainer().getType(), EntityType.CONTAINER);
  }

  @Test
  public void testMapMLTrainingProperties() throws Exception {
    // Create MLTrainingRunProperties
    MLTrainingRunProperties trainingProperties = new MLTrainingRunProperties();
    trainingProperties.setId("test-run-id");
    trainingProperties.setOutputUrls(new StringArray("s3://test-bucket/model"));

    // Add ML training properties aspect
    addAspect(Constants.ML_TRAINING_RUN_PROPERTIES_ASPECT_NAME, trainingProperties);

    DataProcessInstance instance = DataProcessInstanceMapper.map(null, entityResponse);

    assertNotNull(instance);
    assertEquals(instance.getMlTrainingRunProperties().getId(), "test-run-id");
    assertEquals(
        instance.getMlTrainingRunProperties().getOutputUrls().get(0), "s3://test-bucket/model");
  }

  private void addAspect(String aspectName, RecordTemplate aspect) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(aspect.data()));
    entityResponse.getAspects().put(aspectName, envelopedAspect);
  }
}
