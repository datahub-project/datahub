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

import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.DataOrigin;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.FailedMetadataInventoryEvent;
import com.linkedin.events.metadata.MetadataInventoryEvent;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.dao.DaoFactory;
import wherehows.dao.table.DictDatasetDao;
import wherehows.dao.view.DatasetViewDao;
import wherehows.common.exceptions.UnauthorizedException;
import wherehows.utils.ProcessorUtil;

import static wherehows.util.UrnUtil.*;


@Slf4j
public class MetadataInventoryProcessor extends KafkaMessageProcessor {

  private final Set<String> _whitelistActors;

  private final DatasetViewDao _datasetViewDao;

  private final DictDatasetDao _dictDatasetDao;

  public MetadataInventoryProcessor(Config config, DaoFactory daoFactory, String producerTopic,
      KafkaProducer<String, IndexedRecord> producer) {
    super(producerTopic, producer);

    _datasetViewDao = daoFactory.getDatasetViewDao();
    _dictDatasetDao = daoFactory.getDictDatasetDao();

    _whitelistActors = ProcessorUtil.getWhitelistedActors(config, "whitelist.mie");

    log.info("MIE whitelist: " + _whitelistActors);
  }

  /**
   * Process a MetadataChangeEvent record
   * @param indexedRecord IndexedRecord
   */
  public void process(IndexedRecord indexedRecord) {
    if (indexedRecord == null || indexedRecord.getClass() != MetadataInventoryEvent.class) {
      throw new IllegalArgumentException("Invalid record");
    }

    log.debug("Processing Metadata Inventory Event record.");

    final MetadataInventoryEvent event = (MetadataInventoryEvent) indexedRecord;
    try {
      processEvent(event);
    } catch (Exception exception) {
      log.error("MIE Processor Error:", exception);
      log.error("Message content: {}", event.toString());
      sendMessage(newFailedEvent(event, exception));
    }
  }

  public void processEvent(MetadataInventoryEvent event) throws Exception {
    final ChangeAuditStamp changeAuditStamp = event.changeAuditStamp;
    String actorUrn = changeAuditStamp.actorUrn == null ? null : changeAuditStamp.actorUrn.toString();
    if (_whitelistActors != null && !_whitelistActors.contains(actorUrn)) {
      throw new UnauthorizedException("Actor " + actorUrn + " not in whitelist, skip processing");
    }

    final String platformUrn = event.dataPlatformUrn.toString();

    final String platform = getUrnEntity(platformUrn);

    final DataOrigin origin = event.dataOrigin;

    final String namespace = event.namespace.toString();

    log.info("Processing MIE for " + platform + " " + origin + " " + namespace);

    final List<Pattern> exclusions =
        event.exclusionPatterns.stream().map(s -> Pattern.compile(s.toString())).collect(Collectors.toList());

    final List<String> names = event.nativeNames.stream().map(CharSequence::toString).collect(Collectors.toList());
    log.info("new datasets: " + names);

    final List<String> existingDatasets = _datasetViewDao.listFullNames(platform, origin.name(), namespace);
    log.info("existing datasets: " + existingDatasets);

    for (String removedDataset : ProcessorUtil.listDiffWithExclusion(existingDatasets, names, exclusions)) {
      try {
        DatasetIdentifier identifier = new DatasetIdentifier();
        identifier.dataPlatformUrn = platformUrn;
        identifier.dataOrigin = origin;
        identifier.nativeName = removedDataset;

        _dictDatasetDao.setDatasetRemoved(identifier, true, changeAuditStamp);
        log.info("set " + removedDataset + " removed");
      } catch (Exception e) {
        log.error("Fail to mark dataset " + removedDataset + " as removed", e);
      }
    }
  }

  private FailedMetadataInventoryEvent newFailedEvent(MetadataInventoryEvent event, Throwable throwable) {
    FailedMetadataInventoryEvent failedEvent = new FailedMetadataInventoryEvent();
    failedEvent.time = System.currentTimeMillis();
    failedEvent.error = ExceptionUtils.getStackTrace(throwable);
    failedEvent.metadataInventoryEvent = event;
    return failedEvent;
  }
}
