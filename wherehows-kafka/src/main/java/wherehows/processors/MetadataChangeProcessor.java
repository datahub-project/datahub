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
import com.linkedin.events.metadata.FailedMetadataChangeEvent;
import com.linkedin.events.metadata.MetadataChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

  private final int MAX_DATASET_NAME_LENGTH = 400;

  public MetadataChangeProcessor(DaoFactory daoFactory, String producerTopic,
      KafkaProducer<String, IndexedRecord> producer) {
    super(daoFactory, producerTopic, producer);
  }

  /**
   * Process a MetadataChangeEvent record
   * @param indexedRecord IndexedRecord
   */
  public void process(IndexedRecord indexedRecord) {
    if (indexedRecord == null || indexedRecord.getClass() != MetadataChangeEvent.class) {
      throw new IllegalArgumentException("Invalid record");
    }

    log.debug("Processing Metadata Change Event record.");

    MetadataChangeEvent event = (MetadataChangeEvent) indexedRecord;
    try {
      processEvent(event);
    } catch (Exception exception) {
      log.error("MCE Processor Error:", exception);
      log.error("Message content: {}", event.toString());
      this.PRODUCER.send(new ProducerRecord(_producerTopic, newFailedEvent(event, exception)));
    }
  }

  private void processEvent(MetadataChangeEvent event) throws Exception {
    final KafkaAuditHeader auditHeader = event.auditHeader;
    if (auditHeader == null) {
      throw new Exception("Missing Kafka Audit header: " + event.toString());
    }

    final DatasetIdentifier identifier = event.datasetIdentifier;
    log.debug("MCE: " + identifier + " TS: " + auditHeader.time);
    final ChangeAuditStamp changeAuditStamp = event.changeAuditStamp;
    final ChangeType changeType = changeAuditStamp.type;

    if (changeType == ChangeType.DELETE) {
      // TODO: delete dataset
      throw new Exception("Dataset deletion not yet implemented: " + identifier);
    }

    // check dataset name length to be within limit. Otherwise, save to DB will fail.
    if (identifier.nativeName.length() > MAX_DATASET_NAME_LENGTH) {
      throw new Exception("Dataset name too long: " + identifier);
    }

    // create or update dataset
    DictDataset ds =
        _dictDatasetDao.insertUpdateDataset(identifier, changeAuditStamp, event.datasetProperty, event.schema,
            event.deploymentInfo, toStringList(event.tags), event.capacity, event.partitionSpec);

    // if schema is not null, insert or update schema
    if (event.schema != null) {
      _fieldDetailDao.insertUpdateDatasetFields(identifier, ds.getId(), event.datasetProperty, changeAuditStamp,
          event.schema);
    }

    // if owners are not null, insert or update owner
    if (event.owners != null) {
      _ownerDao.insertUpdateOwnership(identifier, ds.getId(), changeAuditStamp, event.owners);
    }

    // if compliance is not null, insert or update compliance
    if (event.compliancePolicy != null) {
      _complianceDao.insertUpdateCompliance(identifier, ds.getId(), changeAuditStamp, event.compliancePolicy);
    }

    // if suggested compliance is not null, insert or update suggested compliance
    if (event.suggestedCompliancePolicy != null) {
      _complianceDao.insertUpdateSuggestedCompliance(identifier, ds.getId(), changeAuditStamp,
          event.suggestedCompliancePolicy);
    }
  }

  private FailedMetadataChangeEvent newFailedEvent(MetadataChangeEvent event, Throwable throwable) {
    FailedMetadataChangeEvent faileEvent = new FailedMetadataChangeEvent();
    faileEvent.time = System.currentTimeMillis();
    faileEvent.error = ExceptionUtils.getStackTrace(throwable);
    faileEvent.metadataChangeEvent = event;
    return faileEvent;
  }
}
