package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.ml.metadata.MLModelGroupProperties;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class MLModelGroupPropertiesMapperTest {

  @Test
  public void testMapMLModelGroupProperties() throws URISyntaxException {
    // Create backend ML Model Group Properties
    MLModelGroupProperties input = new MLModelGroupProperties();

    // Set description
    input.setDescription("a ml trust model group");

    // Set Name
    input.setName("ML trust model group");

    // Create URN
    Urn groupUrn =
        Urn.createFromString(
            "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,another-group,PROD)");

    // Map the properties
    com.linkedin.datahub.graphql.generated.MLModelGroupProperties result =
        MLModelGroupPropertiesMapper.map(null, input, groupUrn);

    // Verify mapped properties
    assertNotNull(result);
    assertEquals(result.getDescription(), "a ml trust model group");
    assertEquals(result.getName(), "ML trust model group");

    // Verify lineage info is null as in the mock data
    assertNotNull(result.getMlModelLineageInfo());
    assertNull(result.getMlModelLineageInfo().getTrainingJobs());
    assertNull(result.getMlModelLineageInfo().getDownstreamJobs());
  }

  @Test
  public void testMapWithMinimalProperties() throws URISyntaxException {
    // Create backend ML Model Group Properties with minimal information
    MLModelGroupProperties input = new MLModelGroupProperties();

    // Create URN
    Urn groupUrn =
        Urn.createFromString(
            "urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,another-group,PROD)");

    // Map the properties
    com.linkedin.datahub.graphql.generated.MLModelGroupProperties result =
        MLModelGroupPropertiesMapper.map(null, input, groupUrn);

    // Verify basic mapping with minimal properties
    assertNotNull(result);
    assertNull(result.getDescription());

    // Verify lineage info is null
    assertNotNull(result.getMlModelLineageInfo());
    assertNull(result.getMlModelLineageInfo().getTrainingJobs());
    assertNull(result.getMlModelLineageInfo().getDownstreamJobs());
  }
}
