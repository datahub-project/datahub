/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package metastore.client;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.AutoPurge;
import com.linkedin.dataset.DatasetPrivacyCompliancePoliciesCreateRequestBuilder;
import com.linkedin.dataset.DatasetPrivacyCompliancePoliciesFindByDatasetRequestBuilder;
import com.linkedin.dataset.DatasetPrivacyCompliancePoliciesGetRequestBuilder;
import com.linkedin.dataset.DatasetPrivacyCompliancePoliciesRequestBuilders;
import com.linkedin.dataset.LimitedRetention;
import com.linkedin.dataset.ManualPurge;
import com.linkedin.dataset.PrivacyCompliancePolicy;
import com.linkedin.dataset.PrivacyCompliancePolicyKey;
import com.linkedin.dataset.PurgeNotApplicable;
import com.linkedin.dataset.SecurityClassification;
import com.linkedin.dataset.SecurityFieldSpec;
import com.linkedin.dataset.SecurityFieldSpecArray;
import com.linkedin.dataset.SecurityMetadata;
import com.linkedin.dataset.SecurityMetadataKey;
import com.linkedin.dataset.SecurityMetadataRequestBuilders;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.FindRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdResponse;
import java.util.List;
import java.util.Map;

import static metastore.util.UrnUtil.*;


public class PrivacySecurity {

  private final RestClient _client;

  private static final DatasetPrivacyCompliancePoliciesRequestBuilders _privacyComplianceBuilder =
      new DatasetPrivacyCompliancePoliciesRequestBuilders();

  private static final SecurityMetadataRequestBuilders _securityBuilder = new SecurityMetadataRequestBuilders();

  public PrivacySecurity(RestClient metadataStoreClient) {
    _client = metadataStoreClient;
  }

  public PrivacyCompliancePolicy getPrivacyCompliancePolicy(String platformName, String datasetName, String origin,
      long version) throws Exception {

    DatasetUrn urn = toDatasetUrn(platformName, datasetName, origin);

    PrivacyCompliancePolicyKey key = new PrivacyCompliancePolicyKey().setDataset(urn).setVersion(version);

    DatasetPrivacyCompliancePoliciesGetRequestBuilder builder = _privacyComplianceBuilder.get();
    Request<PrivacyCompliancePolicy> req = builder.id(new ComplexResourceKey<>(key, new EmptyRecord())).build();

    ResponseFuture<PrivacyCompliancePolicy> responseFuture = _client.sendRequest(req);
    return responseFuture.getResponse().getEntity();
  }

  public PrivacyCompliancePolicy getPrivacyCompliancePolicyLatest(String platformName, String datasetName,
      String origin) throws Exception {

    DatasetUrn urn = toDatasetUrn(platformName, datasetName, origin);

    DatasetPrivacyCompliancePoliciesFindByDatasetRequestBuilder builder = _privacyComplianceBuilder.findByDataset();
    FindRequest<PrivacyCompliancePolicy> req = builder.datasetParam(urn).build();

    ResponseFuture<CollectionResponse<PrivacyCompliancePolicy>> responseFuture = _client.sendRequest(req);
    long version = 0;
    PrivacyCompliancePolicy latestPolicy = null;
    for (PrivacyCompliancePolicy record : responseFuture.getResponse().getEntity().getElements()) {
      if (record.getVersion() > version) {
        latestPolicy = record;
        version = record.getVersion();
      }
    }
    return latestPolicy;
  }

  public PrivacyCompliancePolicyKey createPrivacyCompliancePolicy(String user, String platformName, String datasetName,
      String origin, String purgeType, List<String> purgeFields) throws Exception {
    CorpuserUrn actor = toCorpuserUrn(user);

    PrivacyCompliancePolicy.PurgeMechanism purgeMechanism = new PrivacyCompliancePolicy.PurgeMechanism();
    switch (purgeType) {
      case "AUTO_PURGE":
        purgeMechanism.setAutoPurge(new AutoPurge().setOwnerFields(new StringArray(purgeFields)));
        break;
      case "LIMITED_RETENTION":
        purgeMechanism.setLimitedRetention(new LimitedRetention().setOwnerFields(new StringArray(purgeFields)));
        break;
      case "CUSTOM_PURGE":
        purgeMechanism.setManualPurge(new ManualPurge().setOwnerFields(new StringArray(purgeFields)));
        break;
      case "PURGE_NOT_APPLICABLE":
        purgeMechanism.setPurgeNotApplicable(new PurgeNotApplicable());
        break;
      default:
        throw new IllegalArgumentException("Invalid privacy compliance purge type: " + purgeType);
    }

    DatasetUrn datasetUrn = toDatasetUrn(platformName, datasetName, origin);

    DatasetPrivacyCompliancePoliciesCreateRequestBuilder builder = _privacyComplianceBuilder.create();
    PrivacyCompliancePolicy policy =
        new PrivacyCompliancePolicy().setCreated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()))
            .setLastModified(new AuditStamp().setActor(actor))
            .setPurgeMechanism(purgeMechanism)
            .setDataset(datasetUrn);

    CreateIdRequest<ComplexResourceKey<PrivacyCompliancePolicyKey, EmptyRecord>, PrivacyCompliancePolicy> req =
        builder.input(policy).actorParam(actor).build();
    IdResponse<ComplexResourceKey<PrivacyCompliancePolicyKey, EmptyRecord>> resp =
        _client.sendRequest(req).getResponseEntity();

    return resp.getId().getKey();
  }

  public SecurityMetadata getSecurityMetadata(String platformName, String datasetName, String origin, long version)
      throws Exception {

    DatasetUrn urn = toDatasetUrn(platformName, datasetName, origin);

    SecurityMetadataKey key = new SecurityMetadataKey().setDataset(urn).setVersion(version);

    Request<SecurityMetadata> req = _securityBuilder.get().id(new ComplexResourceKey<>(key, new EmptyRecord())).build();

    ResponseFuture<SecurityMetadata> responseFuture = _client.sendRequest(req);
    return responseFuture.getResponse().getEntity();
  }

  public SecurityMetadata getSecurityMetadataLatest(String platformName, String datasetName, String origin)
      throws Exception {

    DatasetUrn urn = toDatasetUrn(platformName, datasetName, origin);

    FindRequest<SecurityMetadata> req = _securityBuilder.findByDataset().datasetParam(urn).build();

    ResponseFuture<CollectionResponse<SecurityMetadata>> responseFuture = _client.sendRequest(req);
    long version = 0;
    SecurityMetadata latestSecurity = null;
    for (SecurityMetadata record : responseFuture.getResponse().getEntity().getElements()) {
      if (record.getVersion() > version) {
        latestSecurity = record;
        version = record.getVersion();
      }
    }
    return latestSecurity;
  }

  /**
   * Create SecurityMetadata
   * @param user String corp user
   * @param platformName String
   * @param datasetName String
   * @param origin String
   * @param fieldSpecs Map(String fieldName : String classification)
   * @return SecurityMetadataKey
   * @throws Exception
   */
  public SecurityMetadataKey createSecurityMetadata(String user, String platformName, String datasetName, String origin,
      Map<String, String> fieldSpecs) throws Exception {
    CorpuserUrn actor = toCorpuserUrn(user);

    DatasetUrn datasetUrn = toDatasetUrn(platformName, datasetName, origin);

    SecurityFieldSpecArray securityFields = new SecurityFieldSpecArray();
    for (Map.Entry<String, String> entry : fieldSpecs.entrySet()) {
      securityFields.add(new SecurityFieldSpec().setSchemaField(entry.getKey())
          .setClassification(toSecurityClassification(entry.getValue())));
    }

    SecurityClassification datasetSecurity = getHighestSecurityClassification(fieldSpecs.values());

    SecurityMetadata security =
        new SecurityMetadata().setCreated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()))
            .setLastModified(new AuditStamp().setActor(actor))
            .setDatasetSecurityClassification(datasetSecurity)
            .setSensitiveFields(securityFields)
            .setDataset(datasetUrn);

    CreateIdRequest<ComplexResourceKey<SecurityMetadataKey, EmptyRecord>, SecurityMetadata> req =
        _securityBuilder.create().input(security).build();
    IdResponse<ComplexResourceKey<SecurityMetadataKey, EmptyRecord>> resp =
        _client.sendRequest(req).getResponseEntity();

    return resp.getId().getKey();
  }
}
