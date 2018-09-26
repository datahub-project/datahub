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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.CompliancePolicy;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.RetentionPolicy;
import com.linkedin.events.metadata.SuggestedCompliancePolicy;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import wherehows.models.table.DictDataset;
import wherehows.models.table.DsCompliance;
import wherehows.models.view.DatasetCompliance;
import wherehows.models.view.DatasetFieldEntity;
import wherehows.models.view.DatasetRetention;
import wherehows.models.view.DsComplianceSuggestion;

import static wherehows.util.JsonUtil.*;
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

  @Nullable
  public DatasetCompliance getDatasetComplianceByDatasetId(int datasetId, @Nonnull String datasetUrn) throws Exception {
    return dsComplianceToDatasetCompliance(findComplianceById(datasetId));
  }

  @Nonnull
  public DatasetCompliance getDatasetComplianceByUrn(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet, use getDatasetComplianceByDatasetId");
  }

  public void updateDatasetCompliance(@Nonnull DatasetCompliance record, @Nonnull String user) throws Exception {
    DsCompliance compliance = datasetComplianceToDsCompliance(record);
    compliance.setModifiedBy(user);

    update(compliance);
  }

  public void updateDatasetComplianceByUrn(@Nonnull DatasetCompliance record, @Nonnull String user) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet, use updateDatasetCompliance");
  }

  /**
   * Insert / update dataset compliance table given information from MetadataChangeEvent
   * @param identifier DatasetIdentifier
   * @param dataset DictDataset
   * @param auditStamp ChangeAuditStamp
   * @param compliance MCE CompliancePolicy
   * @throws Exception
   */
  public void insertUpdateCompliance(@Nonnull DatasetIdentifier identifier, @Nullable DictDataset dataset,
      @Nonnull ChangeAuditStamp auditStamp, @Nullable CompliancePolicy compliance, @Nullable RetentionPolicy retention)
      throws Exception {
    String datasetUrn = toWhDatasetUrn(identifier);

    if (dataset == null) {
      throw new RuntimeException("Fail to update dataset compliance, dataset is NULL.");
    }
    int datasetId = dataset.getId();

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

    fillDsComplianceByCompliancePolicy(dsCompliance, compliance, retention, datasetUrn, actor);

    update(dsCompliance);
  }

  /**
   * Fill in DsCompliance information from MCE CompliancePolicy
   * @param dsCompliance DsCompliance
   * @param compliance CompliancePolicy
   * @param retention RetentionPolicy
   * @param datasetUrn String
   * @param actor String
   */
  public void fillDsComplianceByCompliancePolicy(@Nonnull DsCompliance dsCompliance,
      @Nullable CompliancePolicy compliance, @Nullable RetentionPolicy retention, @Nonnull String datasetUrn,
      @Nonnull String actor) {
    dsCompliance.setDatasetUrn(datasetUrn);

    // use retention to override compliance
    // retention and compliance can't both be null
    String purgeType = retention != null ? retention.purgeType.name() : compliance.compliancePurgeType.name();
    CharSequence purgeNote = retention != null ? retention.purgeNote : compliance.compliancePurgeNote;
    dsCompliance.setCompliancePurgeType(purgeType);
    if (purgeNote != null) {
      dsCompliance.setCompliancePurgeNote(purgeNote.toString());
    }

    if (compliance != null) {
      dsCompliance.setConfidentiality(compliance.datasetConfidentiality.name());
      dsCompliance.setDatasetClassification(compliance.datasetClassification.toString());
      if (compliance.complianceEntities != null) {
        dsCompliance.setComplianceEntities(compliance.complianceEntities.toString());
      }
    }

    dsCompliance.setModifiedBy(actor);
  }

  @Nullable
  public DsComplianceSuggestion findComplianceSuggestionByUrn(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Compliance Suggestion not implemented.");
  }

  @Nullable
  public DsComplianceSuggestion getComplianceSuggestion(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Compliance Suggestion not implemented.");
  }

  /**
   * Insert / update dataset suggested compliance data from MetadataChangeEvent
   * @param identifier DatasetIdentifier
   * @param dataset DictDataset
   * @param auditStamp ChangeAuditStamp
   * @param suggestion MCE SuggestedCompliancePolicy
   * @throws Exception
   */
  public void insertUpdateSuggestedCompliance(@Nonnull DatasetIdentifier identifier, @Nullable DictDataset dataset,
      @Nonnull ChangeAuditStamp auditStamp, @Nonnull SuggestedCompliancePolicy suggestion) throws Exception {
    // TODO: write suggested compliance information to DB
    throw new UnsupportedOperationException("Compliance Suggestion not implemented.");
  }

  public void sendSuggestedComplianceFeedback(@Nonnull String datasetUrn, @Nonnull String uid, @Nonnull String feedback)
      throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  public DatasetRetention getDatasetRetention(@Nonnull String datasetUrn) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void updateDatasetRetention(@Nonnull DatasetRetention record, @Nonnull String user) throws Exception {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Convert DatasetCompliance view to DsCompliance entity. Serialize certain fields using json.
   * @param compliance DatasetCompliance
   * @return DsCompliance
   * @throws JsonProcessingException
   */
  public DsCompliance datasetComplianceToDsCompliance(DatasetCompliance compliance) throws JsonProcessingException {
    ObjectMapper om = new ObjectMapper();

    DsCompliance dsCompliance = new DsCompliance();
    dsCompliance.setDatasetId(compliance.getDatasetId());
    dsCompliance.setDatasetUrn(compliance.getDatasetUrn());
    dsCompliance.setCompliancePurgeType(compliance.getComplianceType());
    dsCompliance.setCompliancePurgeNote(compliance.getCompliancePurgeNote());
    dsCompliance.setComplianceEntities(om.writeValueAsString(compliance.getComplianceEntities()));
    dsCompliance.setConfidentiality(compliance.getConfidentiality());
    dsCompliance.setDatasetClassification(om.writeValueAsString(compliance.getDatasetClassification()));
    dsCompliance.setModifiedBy(compliance.getModifiedBy());
    return dsCompliance;
  }

  /**
   * Convert DsCompliance entity to DatasetCompliance view. De-serialize certain fields using json.
   * @param dsCompliance DsCompliance
   * @return DatasetCompliance
   * @throws IOException
   */
  public DatasetCompliance dsComplianceToDatasetCompliance(@Nonnull DsCompliance dsCompliance) throws IOException {
    DatasetCompliance compliance = new DatasetCompliance();
    compliance.setDatasetId(dsCompliance.getDatasetId());
    compliance.setDatasetUrn(dsCompliance.getDatasetUrn());
    compliance.setComplianceType(dsCompliance.getCompliancePurgeType());
    compliance.setCompliancePurgeNote(dsCompliance.getCompliancePurgeNote());
    compliance.setComplianceEntities(
        jsonToTypedObject(dsCompliance.getComplianceEntities(), new TypeReference<List<DatasetFieldEntity>>() {
        }));
    compliance.setConfidentiality(dsCompliance.getConfidentiality());
    compliance.setDatasetClassification(
        jsonToTypedObject(dsCompliance.getDatasetClassification(), new TypeReference<Map<String, Object>>() {
        }));
    compliance.setModifiedBy(dsCompliance.getModifiedBy());
    compliance.setModifiedTime(1000L * dsCompliance.getModifiedTime());
    return compliance;
  }
}
