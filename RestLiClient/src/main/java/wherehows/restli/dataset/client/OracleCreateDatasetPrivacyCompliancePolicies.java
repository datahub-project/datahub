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

package wherehows.restli.dataset.client;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.MemberUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.ForeignKeySpecMap;
import com.linkedin.dataset.PrivacyCompliancePolicyKey;
import com.linkedin.dataset.SchemaFieldArray;
import com.linkedin.dataset.SchemaMetadata;
import com.linkedin.dataset.SchemaMetadataKey;
import com.linkedin.dataset.SchemaMetadataRequestBuilders;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.linkedin.dataset.DatasetPrivacyCompliancePoliciesRequestBuilders;
import com.linkedin.dataset.PrivacyCompliancePolicy;
import com.linkedin.dataset.*;

import static wherehows.restli.dataset.client.OracleCreateDataSet.toDataPlatformUrn;
import static wherehows.restli.dataset.client.OracleCreateDataSet.toDatasetUrn;


public class OracleCreateDatasetPrivacyCompliancePolicies {

  public void create( RestClient _restClient, String iDatasetName, String iUrn, String iCompliancePurgeType)
      throws Exception {

    final MemberUrn ACTOR = MemberUrn.deserialize(iUrn);

    PrivacyCompliancePolicy.PurgeMechanism purgeMechanism = new PrivacyCompliancePolicy.PurgeMechanism();
    switch  (iCompliancePurgeType) {
      case "AUTO_PURGE":
        purgeMechanism.setAutoPurge(new AutoPurge());
        break;
      case "LIMITED_RETENTION":
        purgeMechanism.setLimitedRetention(new LimitedRetention());
      case "CUSTOM_PURGE":
        purgeMechanism.setManualPurge(new ManualPurge());
      case "PURGE_NOT_APPLICABLE":
        purgeMechanism.setPurgeNotApplicable(new PurgeNotApplicable());
      default:
        throw new IllegalArgumentException("Invalid compliance purge type: " + iCompliancePurgeType);
    }

    PrivacyCompliancePolicy policy = new PrivacyCompliancePolicy().setCreated(new AuditStamp().setActor(ACTOR))
        .setLastModified(new AuditStamp().setActor(ACTOR))
        .setPurgeMechanism(purgeMechanism)
        .setDataset(toDatasetUrn(iDatasetName, toDataPlatformUrn("oracle"), FabricType.PROD));


    try {

      IdResponse<ComplexResourceKey<PrivacyCompliancePolicyKey, EmptyRecord>> response = _restClient.sendRequest(
          PRIVACY_COMPLIANCE_POLICIES_REQUEST_BUILDERS.create().input(policy).actorParam(ACTOR).build())
          .getResponseEntity();

      System.out.println("===OracleCreateDatasetPrivacyCompliancePolicies===");
      System.out.println(response.getId().getKey());
    } catch (Exception e) {
      System.out.println(e.fillInStackTrace());
    }

  }

  private com.linkedin.dataset.PrivacyCompliancePolicy createMockPrivacyCompliance() {
    AutoPurge purge = new AutoPurge();
    com.linkedin.dataset.PrivacyCompliancePolicy.PurgeMechanism purgeMechanism =
        new com.linkedin.dataset.PrivacyCompliancePolicy.PurgeMechanism();
    purgeMechanism.setAutoPurge(purge);
    return new com.linkedin.dataset.PrivacyCompliancePolicy().setPurgeMechanism(purgeMechanism);
  }

  private static final DatasetPrivacyCompliancePoliciesRequestBuilders PRIVACY_COMPLIANCE_POLICIES_REQUEST_BUILDERS = new DatasetPrivacyCompliancePoliciesRequestBuilders();

}
