package com.linkedin.datahub.graphql.types.datacontract;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataContractState;
import com.linkedin.datacontract.DataContractStatus;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.DataQualityContractArray;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.FreshnessContractArray;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datacontract.SchemaContractArray;
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import org.testng.annotations.Test;

public class DataContractMapperTest {

  @Test
  public void testMapAllFields() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:dataContract:1");
    Urn dataQualityAssertionUrn = Urn.createFromString("urn:li:assertion:quality");
    Urn dataQualityAssertionUrn2 = Urn.createFromString("urn:li:assertion:quality2");

    Urn freshnessAssertionUrn = Urn.createFromString("urn:li:assertion:freshness");
    Urn schemaAssertionUrn = Urn.createFromString("urn:li:assertion:schema");
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedDataContractProperties = new EnvelopedAspect();
    DataContractProperties dataContractProperties = new DataContractProperties();
    dataContractProperties.setDataQuality(
        new DataQualityContractArray(
            ImmutableList.of(
                new DataQualityContract().setAssertion(dataQualityAssertionUrn),
                new DataQualityContract().setAssertion(dataQualityAssertionUrn2))));
    dataContractProperties.setFreshness(
        new FreshnessContractArray(
            ImmutableList.of(new FreshnessContract().setAssertion(freshnessAssertionUrn))));
    dataContractProperties.setSchema(
        new SchemaContractArray(
            ImmutableList.of(new SchemaContract().setAssertion(schemaAssertionUrn))));

    dataContractProperties.setEntity(datasetUrn);

    envelopedDataContractProperties.setValue(new Aspect(dataContractProperties.data()));

    EnvelopedAspect envelopedDataContractStatus = new EnvelopedAspect();
    DataContractStatus status = new DataContractStatus();
    status.setState(DataContractState.PENDING);
    status.setCustomProperties(new StringMap(ImmutableMap.of("key", "value")));

    envelopedDataContractStatus.setValue(new Aspect(status.data()));
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(
                Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME, envelopedDataContractProperties)));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME,
                envelopedDataContractProperties,
                Constants.DATA_CONTRACT_STATUS_ASPECT_NAME,
                envelopedDataContractStatus)));

    DataContract dataContract = DataContractMapper.mapContract(entityResponse);
    assertNotNull(dataContract);
    assertEquals(dataContract.getUrn(), urn.toString());
    assertEquals(dataContract.getType(), EntityType.DATA_CONTRACT);
    assertEquals(
        dataContract.getStatus().getState(),
        com.linkedin.datahub.graphql.generated.DataContractState.PENDING);
    assertEquals(dataContract.getProperties().getEntityUrn(), datasetUrn.toString());
    assertEquals(dataContract.getProperties().getDataQuality().size(), 2);
    assertEquals(
        dataContract.getProperties().getDataQuality().get(0).getAssertion().getUrn(),
        dataQualityAssertionUrn.toString());
    assertEquals(
        dataContract.getProperties().getDataQuality().get(1).getAssertion().getUrn(),
        dataQualityAssertionUrn2.toString());
    assertEquals(dataContract.getProperties().getFreshness().size(), 1);
    assertEquals(
        dataContract.getProperties().getFreshness().get(0).getAssertion().getUrn(),
        freshnessAssertionUrn.toString());
    assertEquals(dataContract.getProperties().getSchema().size(), 1);
    assertEquals(
        dataContract.getProperties().getSchema().get(0).getAssertion().getUrn(),
        schemaAssertionUrn.toString());
  }

  @Test
  public void testMapRequiredFields() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:dataContract:1");
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedDataContractProperties = new EnvelopedAspect();
    DataContractProperties dataContractProperties = new DataContractProperties();
    dataContractProperties.setEntity(datasetUrn);
    envelopedDataContractProperties.setValue(new Aspect(dataContractProperties.data()));

    EnvelopedAspect envelopedDataContractStatus = new EnvelopedAspect();
    DataContractStatus status = new DataContractStatus();
    status.setState(DataContractState.PENDING);
    status.setCustomProperties(new StringMap(ImmutableMap.of("key", "value")));

    envelopedDataContractStatus.setValue(new Aspect(status.data()));
    entityResponse.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(
                Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME, envelopedDataContractProperties)));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME,
                envelopedDataContractProperties,
                Constants.DATA_CONTRACT_STATUS_ASPECT_NAME,
                envelopedDataContractStatus)));

    DataContract dataContract = DataContractMapper.mapContract(entityResponse);
    assertNotNull(dataContract);
    assertEquals(dataContract.getUrn(), urn.toString());
    assertEquals(dataContract.getType(), EntityType.DATA_CONTRACT);
    assertEquals(
        dataContract.getStatus().getState(),
        com.linkedin.datahub.graphql.generated.DataContractState.PENDING);
    assertEquals(dataContract.getProperties().getEntityUrn(), datasetUrn.toString());
    assertNull(dataContract.getProperties().getDataQuality());
    assertNull(dataContract.getProperties().getSchema());
    assertNull(dataContract.getProperties().getFreshness());
  }

  @Test
  public void testMapNoStatus() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:dataContract:1");
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedDataContractProperties = new EnvelopedAspect();
    DataContractProperties dataContractProperties = new DataContractProperties();
    dataContractProperties.setEntity(datasetUrn);
    envelopedDataContractProperties.setValue(new Aspect(dataContractProperties.data()));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME, envelopedDataContractProperties)));

    DataContract dataContract = DataContractMapper.mapContract(entityResponse);
    assertNotNull(dataContract);
    assertEquals(dataContract.getUrn(), urn.toString());
    assertEquals(dataContract.getType(), EntityType.DATA_CONTRACT);
    assertNull(dataContract.getStatus());
    assertEquals(dataContract.getProperties().getEntityUrn(), datasetUrn.toString());
    assertNull(dataContract.getProperties().getDataQuality());
    assertNull(dataContract.getProperties().getSchema());
    assertNull(dataContract.getProperties().getFreshness());
  }
}
