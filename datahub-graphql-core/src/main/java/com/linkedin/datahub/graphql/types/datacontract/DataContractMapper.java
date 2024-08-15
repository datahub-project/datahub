package com.linkedin.datahub.graphql.types.datacontract;

import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.DataContractProperties;
import com.linkedin.datahub.graphql.generated.DataContractState;
import com.linkedin.datahub.graphql.generated.DataContractStatus;
import com.linkedin.datahub.graphql.generated.DataQualityContract;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FreshnessContract;
import com.linkedin.datahub.graphql.generated.SchemaContract;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DataContractMapper {

  public static DataContract mapContract(@Nonnull final EntityResponse entityResponse) {
    final DataContract result = new DataContract();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATA_CONTRACT);

    final EnvelopedAspect dataContractProperties =
        aspects.get(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME);
    if (dataContractProperties != null) {
      result.setProperties(
          mapProperties(
              new com.linkedin.datacontract.DataContractProperties(
                  dataContractProperties.getValue().data())));
    } else {
      throw new RuntimeException(
          String.format("Data Contract does not exist!. urn: %s", entityResponse.getUrn()));
    }

    final EnvelopedAspect dataContractStatus =
        aspects.get(Constants.DATA_CONTRACT_STATUS_ASPECT_NAME);
    if (dataContractStatus != null) {
      result.setStatus(
          mapStatus(
              new com.linkedin.datacontract.DataContractStatus(
                  dataContractStatus.getValue().data())));
    }

    return result;
  }

  private static DataContractProperties mapProperties(
      final com.linkedin.datacontract.DataContractProperties properties) {
    final DataContractProperties result = new DataContractProperties();
    result.setEntityUrn(properties.getEntity().toString());
    if (properties.hasSchema()) {
      result.setSchema(
          properties.getSchema().stream()
              .map(DataContractMapper::mapSchemaContract)
              .collect(Collectors.toList()));
    }
    if (properties.hasFreshness()) {
      result.setFreshness(
          properties.getFreshness().stream()
              .map(DataContractMapper::mapFreshnessContract)
              .collect(Collectors.toList()));
    }
    if (properties.hasDataQuality()) {
      result.setDataQuality(
          properties.getDataQuality().stream()
              .map(DataContractMapper::mapDataQualityContract)
              .collect(Collectors.toList()));
    }
    return result;
  }

  private static DataContractStatus mapStatus(
      final com.linkedin.datacontract.DataContractStatus status) {
    final DataContractStatus result = new DataContractStatus();
    result.setState(DataContractState.valueOf(status.getState().toString()));
    return result;
  }

  private static SchemaContract mapSchemaContract(
      final com.linkedin.datacontract.SchemaContract schemaContract) {
    final SchemaContract result = new SchemaContract();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(schemaContract.getAssertion().toString());
    result.setAssertion(partialAssertion);
    return result;
  }

  private static FreshnessContract mapFreshnessContract(
      final com.linkedin.datacontract.FreshnessContract freshnessContract) {
    final FreshnessContract result = new FreshnessContract();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(freshnessContract.getAssertion().toString());
    result.setAssertion(partialAssertion);
    return result;
  }

  private static DataQualityContract mapDataQualityContract(
      final com.linkedin.datacontract.DataQualityContract qualityContract) {
    final DataQualityContract result = new DataQualityContract();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(qualityContract.getAssertion().toString());
    result.setAssertion(partialAssertion);
    return result;
  }

  private DataContractMapper() {}
}
