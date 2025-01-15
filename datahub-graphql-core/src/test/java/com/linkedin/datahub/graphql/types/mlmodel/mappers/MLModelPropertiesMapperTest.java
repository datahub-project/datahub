package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.MLFeatureUrnArray;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.VersionTag;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.MLFeatureUrn;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.ml.metadata.MLHyperParam;
import com.linkedin.ml.metadata.MLHyperParamArray;
import com.linkedin.ml.metadata.MLMetric;
import com.linkedin.ml.metadata.MLMetricArray;
import com.linkedin.ml.metadata.MLModelProperties;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class MLModelPropertiesMapperTest {

  @Test
  public void testMapMLModelProperties() throws URISyntaxException {
    MLModelProperties input = new MLModelProperties();

    // Set basic properties
    input.setName("TestModel");
    input.setDescription("A test ML model");
    input.setType("Classification");

    // Set version
    VersionTag versionTag = new VersionTag();
    versionTag.setVersionTag("1.0.0");
    input.setVersion(versionTag);

    // Set external URL
    Url externalUrl = new Url("https://example.com/model");
    input.setExternalUrl(externalUrl);

    // Set created and last modified timestamps
    TimeStamp createdTimeStamp = new TimeStamp();
    createdTimeStamp.setTime(1000L);
    Urn userUrn = Urn.createFromString("urn:li:corpuser:test");
    createdTimeStamp.setActor(userUrn);
    input.setCreated(createdTimeStamp);

    TimeStamp lastModifiedTimeStamp = new TimeStamp();
    lastModifiedTimeStamp.setTime(2000L);
    lastModifiedTimeStamp.setActor(userUrn);
    input.setLastModified(lastModifiedTimeStamp);

    // Set custom properties
    StringMap customProps = new StringMap();
    customProps.put("key1", "value1");
    customProps.put("key2", "value2");
    input.setCustomProperties(customProps);

    // Set hyper parameters
    MLHyperParamArray hyperParams = new MLHyperParamArray();
    MLHyperParam hyperParam1 = new MLHyperParam();
    hyperParam1.setName("learning_rate");
    hyperParam1.setValue("0.01");
    hyperParams.add(hyperParam1);
    input.setHyperParams(hyperParams);

    // Set training metrics
    MLMetricArray trainingMetrics = new MLMetricArray();
    MLMetric metric1 = new MLMetric();
    metric1.setName("accuracy");
    metric1.setValue("0.95");
    trainingMetrics.add(metric1);
    input.setTrainingMetrics(trainingMetrics);

    // Set ML features
    MLFeatureUrnArray mlFeatures = new MLFeatureUrnArray();
    MLFeatureUrn featureUrn = MLFeatureUrn.createFromString("urn:li:mlFeature:(dataset,feature)");
    mlFeatures.add(featureUrn);
    input.setMlFeatures(mlFeatures);

    // Set tags
    StringArray tags = new StringArray();
    tags.add("tag1");
    tags.add("tag2");
    input.setTags(tags);

    // Set training and downstream jobs
    input.setTrainingJobs(
        new com.linkedin.common.UrnArray(Urn.createFromString("urn:li:dataJob:train")));
    input.setDownstreamJobs(
        new com.linkedin.common.UrnArray(Urn.createFromString("urn:li:dataJob:predict")));

    // Create ML Model URN
    MLModelUrn modelUrn =
        MLModelUrn.createFromString(
            "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,unittestmodel,PROD)");

    // Map the properties
    com.linkedin.datahub.graphql.generated.MLModelProperties result =
        MLModelPropertiesMapper.map(null, input, modelUrn);

    // Verify mapped properties
    assertNotNull(result);
    assertEquals(result.getName(), "TestModel");
    assertEquals(result.getDescription(), "A test ML model");
    assertEquals(result.getType(), "Classification");
    assertEquals(result.getVersion(), "1.0.0");
    assertEquals(result.getExternalUrl(), "https://example.com/model");

    // Verify audit stamps
    assertNotNull(result.getCreated());
    assertEquals(result.getCreated().getTime().longValue(), 1000L);
    assertEquals(result.getCreated().getActor(), userUrn.toString());

    assertNotNull(result.getLastModified());
    assertEquals(result.getLastModified().getTime().longValue(), 2000L);
    assertEquals(result.getLastModified().getActor(), userUrn.toString());

    // Verify custom properties
    assertNotNull(result.getCustomProperties());

    // Verify hyper parameters
    assertNotNull(result.getHyperParams());
    assertEquals(result.getHyperParams().size(), 1);
    assertEquals(result.getHyperParams().get(0).getName(), "learning_rate");
    assertEquals(result.getHyperParams().get(0).getValue(), "0.01");

    // Verify training metrics
    assertNotNull(result.getTrainingMetrics());
    assertEquals(result.getTrainingMetrics().size(), 1);
    assertEquals(result.getTrainingMetrics().get(0).getName(), "accuracy");
    assertEquals(result.getTrainingMetrics().get(0).getValue(), "0.95");

    // Verify ML features
    assertNotNull(result.getMlFeatures());
    assertEquals(result.getMlFeatures().size(), 1);
    assertEquals(result.getMlFeatures().get(0), featureUrn.toString());

    // Verify tags
    assertNotNull(result.getTags());
    assertEquals(result.getTags().get(0), "tag1");
    assertEquals(result.getTags().get(1), "tag2");

    // Verify lineage info
    assertNotNull(result.getMlModelLineageInfo());
    assertEquals(result.getMlModelLineageInfo().getTrainingJobs().size(), 1);
    assertEquals(result.getMlModelLineageInfo().getTrainingJobs().get(0), "urn:li:dataJob:train");
    assertEquals(result.getMlModelLineageInfo().getDownstreamJobs().size(), 1);
    assertEquals(
        result.getMlModelLineageInfo().getDownstreamJobs().get(0), "urn:li:dataJob:predict");
  }

  @Test
  public void testMapWithMissingName() throws URISyntaxException {
    MLModelProperties input = new MLModelProperties();
    MLModelUrn modelUrn =
        MLModelUrn.createFromString(
            "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,missingnamemodel,PROD)");

    com.linkedin.datahub.graphql.generated.MLModelProperties result =
        MLModelPropertiesMapper.map(null, input, modelUrn);

    // Verify that name is extracted from URN when not present in input
    assertEquals(result.getName(), "missingnamemodel");
  }

  @Test
  public void testMapWithMinimalProperties() throws URISyntaxException {
    MLModelProperties input = new MLModelProperties();
    MLModelUrn modelUrn =
        MLModelUrn.createFromString(
            "urn:li:mlModel:(urn:li:dataPlatform:sagemaker,minimalmodel,PROD)");

    com.linkedin.datahub.graphql.generated.MLModelProperties result =
        MLModelPropertiesMapper.map(null, input, modelUrn);

    // Verify basic mapping with minimal properties
    assertNotNull(result);
    assertEquals(result.getName(), "minimalmodel");
    assertNull(result.getDescription());
    assertNull(result.getType());
    assertNull(result.getVersion());
  }
}
