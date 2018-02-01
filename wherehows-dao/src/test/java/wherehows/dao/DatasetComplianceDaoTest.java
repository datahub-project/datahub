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
package wherehows.dao;

import com.linkedin.events.metadata.ComplianceDataType;
import com.linkedin.events.metadata.ComplianceEntity;
import com.linkedin.events.metadata.CompliancePolicy;
import com.linkedin.events.metadata.CompliancePurgeType;
import com.linkedin.events.metadata.DatasetClassification;
import com.linkedin.events.metadata.SecurityClassification;
import java.util.ArrayList;
import java.util.Arrays;
import org.testng.annotations.Test;
import wherehows.dao.table.DatasetComplianceDao;
import wherehows.models.table.DsCompliance;
import wherehows.models.view.DatasetCompliance;

import static org.testng.Assert.*;


public class DatasetComplianceDaoTest {

  @Test
  public void testFillDsComplianceByCompliancePolicy() {
    DatasetComplianceDao complianceDao = new DatasetComplianceDao(null);

    DsCompliance dsCompliance = new DsCompliance();
    String datasetUrn = "teradata:///abc/test";
    String actor = "tester";
    CompliancePurgeType purgeType = CompliancePurgeType.PURGE_EXEMPTED;
    String purgeNote = "Purge not needed.";

    CompliancePolicy policy = new CompliancePolicy();
    policy.compliancePurgeType = purgeType;
    policy.compliancePurgeNote = purgeNote;
    policy.datasetClassification = new DatasetClassification();
    policy.datasetConfidentiality = SecurityClassification.LIMITED_DISTRIBUTION;
    policy.complianceEntities = new ArrayList<>();

    complianceDao.fillDsComplianceByCompliancePolicy(dsCompliance, policy, datasetUrn, actor);

    String dsClassification = "{connectionsOrFollowersOrFollowing: false, profile: false, messaging: false, "
        + "thirdPartyIntegrationsInUse: false, activity: false, settings: false, jobApplicationFlow: false, "
        + "enterpriseProduct: false, accountStatus: false, addressBookImports: false, microsoftData: false, "
        + "subsidiaryData: false, otherThirdPartyIntegrations: false, device: false, searchHistory: false, "
        + "courseViewingHistory: false, whoViewedMyProfile: false, profileViewsByMe: false, advertising: false, "
        + "usageOrErrorOrConnectivity: false, otherClickstreamOrBrowsingData: false}";

    assertEquals(dsCompliance.getCompliancePurgeType(), purgeType.toString());
    assertEquals(dsCompliance.getCompliancePurgeNote(), purgeNote);
    assertEquals(dsCompliance.getDatasetClassification().replaceAll("\"", ""), dsClassification);
    assertEquals(dsCompliance.getConfidentiality(), "LIMITED_DISTRIBUTION");
    assertEquals(dsCompliance.getComplianceEntities(), "[]");

    assertEquals(dsCompliance.getDatasetUrn(), datasetUrn);
    assertEquals(dsCompliance.getModifiedBy(), actor);

    String datasetUrn2 = "teradata:///abc/test2";
    String actor2 = "tester2";

    ComplianceEntity entity = new ComplianceEntity();
    entity.fieldPath = "field1";
    entity.complianceDataType = ComplianceDataType.ADDRESS;
    entity.securityClassification = SecurityClassification.CONFIDENTIAL;

    policy.complianceEntities = Arrays.asList(entity);
    policy.datasetConfidentiality = SecurityClassification.HIGHLY_CONFIDENTIAL;

    String complianceEntityStr = "[{fieldPath: field1, complianceDataType: ADDRESS, complianceDataTypeUrn: null, "
        + "fieldFormat: null, securityClassification: CONFIDENTIAL}]";

    complianceDao.fillDsComplianceByCompliancePolicy(dsCompliance, policy, datasetUrn2, actor2);

    assertEquals(dsCompliance.getCompliancePurgeType(), purgeType.toString());
    assertEquals(dsCompliance.getDatasetClassification().replaceAll("\"", ""), dsClassification);
    assertEquals(dsCompliance.getConfidentiality(), "HIGHLY_CONFIDENTIAL");
    assertEquals(dsCompliance.getComplianceEntities().replaceAll("\"", ""), complianceEntityStr);

    assertEquals(dsCompliance.getDatasetUrn(), datasetUrn2);
    assertEquals(dsCompliance.getModifiedBy(), actor2);
    assertEquals(dsCompliance.getModifiedTime(), null);
  }

  @Test
  public void testDatasetComplianceToDsCompliance() throws Exception {
    DatasetComplianceDao complianceDao = new DatasetComplianceDao(null);

    int id = 1;
    String urn = "foo.bar";
    String purgeType = "Auto";
    String purgeNote = "None";
    String confidentiality = "confidential";
    String actor = "me";
    long time = 1000L;

    DatasetCompliance compliance = new DatasetCompliance();
    compliance.setDatasetId(id);
    compliance.setDatasetUrn(urn);
    compliance.setComplianceType(purgeType);
    compliance.setCompliancePurgeNote(purgeNote);
    compliance.setComplianceEntities(new ArrayList<>());
    compliance.setConfidentiality(confidentiality);
    compliance.setModifiedBy(actor);
    compliance.setModifiedTime(time);

    DsCompliance dsCompliance = complianceDao.datasetComplianceToDsCompliance(compliance);

    assertEquals(dsCompliance.getDatasetId(), id);
    assertEquals(dsCompliance.getDatasetUrn(), urn);
    assertEquals(dsCompliance.getCompliancePurgeType(), purgeType);
    assertEquals(dsCompliance.getCompliancePurgeNote(), purgeNote);
    assertEquals(dsCompliance.getComplianceEntities(), "[]");
    assertEquals(dsCompliance.getConfidentiality(), confidentiality);
    assertEquals(dsCompliance.getModifiedBy(), actor);
    assertEquals(dsCompliance.getModifiedTime(), null);
  }

  @Test
  public void testDsComplianceToDatasetCompliance() throws Exception {
    DatasetComplianceDao complianceDao = new DatasetComplianceDao(null);

    int id = 1;
    String urn = "foo.bar";
    String purgeType = "Auto";
    String purgeNote = "None";
    String confidentiality = "confidential";
    String actor = "me";
    int time = 1;

    DsCompliance dsCompliance = new DsCompliance();
    dsCompliance.setDatasetId(id);
    dsCompliance.setDatasetUrn(urn);
    dsCompliance.setCompliancePurgeType(purgeType);
    dsCompliance.setCompliancePurgeNote(purgeNote);
    dsCompliance.setComplianceEntities("[]");
    dsCompliance.setConfidentiality(confidentiality);
    dsCompliance.setModifiedBy(actor);
    dsCompliance.setModifiedTime(time);

    DatasetCompliance compliance = complianceDao.dsComplianceToDatasetCompliance(dsCompliance);

    assertEquals(compliance.getDatasetId(), new Integer(id));
    assertEquals(compliance.getDatasetUrn(), urn);
    assertEquals(compliance.getComplianceType(), purgeType);
    assertEquals(compliance.getCompliancePurgeNote(), purgeNote);
    assertEquals(compliance.getComplianceEntities().size(), 0);
    assertEquals(compliance.getConfidentiality(), confidentiality);
    assertEquals(compliance.getModifiedBy(), actor);
    assertEquals(compliance.getModifiedTime(), new Long(time * 1000));
  }
}
