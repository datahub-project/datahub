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
package wherehows.processors;

import com.linkedin.events.KafkaAuditHeader;
import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.MetadataChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.dao.DaoFactory;
import wherehows.dao.table.DatasetComplianceDao;
import wherehows.dao.table.DatasetOwnerDao;
import wherehows.dao.table.DictDatasetDao;
import wherehows.dao.table.FieldDetailDao;
import wherehows.models.table.DictDataset;

import static wherehows.common.utils.StringUtil.*;


@Slf4j
public class MetadataChangeProcessor extends KafkaMessageProcessor {

  private final DictDatasetDao _dictDatasetDao = DAO_FACTORY.getDictDatasetDao();

  private final FieldDetailDao _fieldDetailDao = DAO_FACTORY.getDictFieldDetailDao();

  private final DatasetOwnerDao _ownerDao = DAO_FACTORY.getDatasteOwnerDao();

  private final DatasetComplianceDao _complianceDao = DAO_FACTORY.getDatasetComplianceDao();

  public MetadataChangeProcessor(DaoFactory daoFactory, KafkaProducer<String, IndexedRecord> producer) {
    super(daoFactory, producer);
  }

  /**
   * Process a MetadataChangeEvent record
   * @param indexedRecord IndexedRecord
   * @throws Exception
   */
  public void process(IndexedRecord indexedRecord) throws Exception {

    if (indexedRecord == null || indexedRecord.getClass() != MetadataChangeEvent.class) {
      throw new IllegalArgumentException("Invalid record");
    }

    log.debug("Processing Metadata Change Event record. ");

    MetadataChangeEvent record = (MetadataChangeEvent) indexedRecord;

    final KafkaAuditHeader auditHeader = record.auditHeader;
    if (auditHeader == null) {
      log.warn("MetadataChangeEvent without auditHeader, abort process. " + record.toString());
      return;
    }

    final DatasetIdentifier identifier = record.datasetIdentifier;
    final ChangeAuditStamp changeAuditStamp = record.changeAuditStamp;
    final ChangeType changeType = changeAuditStamp.type;

    if (changeType == ChangeType.DELETE) {
      // TODO: delete dataset
      log.debug("Dataset Deleted: " + identifier);
      return;
    }

    // create or update dataset
    DictDataset ds =
        _dictDatasetDao.insertUpdateDataset(identifier, changeAuditStamp, record.datasetProperty, record.schema,
            record.deploymentInfo, toStringList(record.tags), record.capacity, record.partitionSpec);

    // if schema is not null, insert or update schema
    if (record.schema != null) {
      _fieldDetailDao.insertUpdateDatasetFields(identifier, ds.getId(), record.datasetProperty, changeAuditStamp,
          record.schema);
    }

    // if owners are not null, insert or update owner
    if (record.owners != null) {
      _ownerDao.insertUpdateOwnership(identifier, ds.getId(), changeAuditStamp, record.owners);
    }

    // if compliance is not null, insert or update compliance
    if (record.compliancePolicy != null) {
      // write compliance info to DB
    }

    // if suggested compliance is not null, insert or update suggested compliance
    if (record.suggestedCompliancePolicy != null) {
      // write suggested compliance info to DB
    }
  }
}
