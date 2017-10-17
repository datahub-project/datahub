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
package wherehows.dao.table;

import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.CompliancePolicy;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.SuggestedCompliancePolicy;
import javax.annotation.Nonnull;
import javax.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import wherehows.models.table.DsCompliance;
import wherehows.models.view.DsComplianceSuggestion;

import static wherehows.util.UrnUtil.*;


@Slf4j
public class DatasetComplianceDao extends BaseDao {

  public DatasetComplianceDao(@Nonnull EntityManagerFactory factory) {
    super(factory);
  }

  public DsCompliance findComplianceByUrn(@Nonnull String datasetUrn) {
    return findBy(DsCompliance.class, "datasetUrn", datasetUrn);
  }

  public DsCompliance findComplianceById(int datasetId) {
    return findBy(DsCompliance.class, "datasetId", datasetId);
  }

  /**
   * Insert / update dataset compliance table given information from MetadataChangeEvent
   * @param identifier DatasetIdentifier
   * @param datasetId int
   * @param auditStamp ChangeAuditStamp
   * @param compliance MCE CompliancePolicy
   * @throws Exception
   */
  public void insertUpdateCompliance(@Nonnull DatasetIdentifier identifier, int datasetId, @Nonnull ChangeAuditStamp auditStamp,
      @Nonnull CompliancePolicy compliance) throws Exception {

    String datasetUrn = toWhDatasetUrn(identifier);

    // find dataset compliance if exist
    DsCompliance dsCompliance = null;
    try {
      dsCompliance = findBy(DsCompliance.class, "datasetId", datasetId);
    } catch (Exception e) {
      log.debug("Can't find dataset compliance for ID: " + datasetId, e.toString());
    }

    if (dsCompliance == null) {
      dsCompliance = new DsCompliance();
      dsCompliance.setDatasetId(datasetId);
    }

    String actor = getUrnEntity(toStringOrNull(auditStamp.actorUrn));

    fillDsComplianceByCompliancePolicy(dsCompliance, compliance, datasetUrn, actor);

    update(dsCompliance);
  }

  /**
   * Fill in DsCompliance information from MCE CompliancePolicy
   * @param dsCompliance DsCompliance
   * @param compliance CompliancePolicy
   * @param datasetUrn String
   * @param actor String
   */
  public void fillDsComplianceByCompliancePolicy(@Nonnull DsCompliance dsCompliance, @Nonnull CompliancePolicy compliance,
      @Nonnull String datasetUrn, @Nonnull String actor) {

    dsCompliance.setDatasetUrn(datasetUrn);
    dsCompliance.setCompliancePurgeType(compliance.compliancePurgeType.name());
    dsCompliance.setConfidentiality(compliance.datasetConfidentiality.name());
    dsCompliance.setDatasetClassification(compliance.datasetClassification.toString());
    if (compliance.complianceEntities != null) {
      dsCompliance.setComplianceEntities(compliance.complianceEntities.toString());
    }

    dsCompliance.setModifiedBy(actor);
  }

  public DsComplianceSuggestion findComplianceSuggestionByUrn(@Nonnull String datasetUrn) {
    throw new UnsupportedOperationException("Compliance Suggestion not implemented.");
  }

  /**
   * Insert / update dataset suggested compliance data from MetadataChangeEvent
   * @param identifier DatasetIdentifier
   * @param datasetId int
   * @param auditStamp ChangeAuditStamp
   * @param suggestion MCE SuggestedCompliancePolicy
   * @throws Exception
   */
  public void insertUpdateSuggestedCompliance(@Nonnull DatasetIdentifier identifier, int datasetId, @Nonnull ChangeAuditStamp auditStamp,
      @Nonnull SuggestedCompliancePolicy suggestion) throws Exception {
    // TODO: write suggested compliance information to DB
    throw new UnsupportedOperationException("Compliance Suggestion not implemented.");
  }
}
