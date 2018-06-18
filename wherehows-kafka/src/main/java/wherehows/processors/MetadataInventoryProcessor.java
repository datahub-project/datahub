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
import com.linkedin.events.metadata.MetadataChangeEvent;
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
import wherehows.common.exceptions.UnauthorizedException;
import wherehows.dao.DaoFactory;
import wherehows.dao.view.DatasetViewDao;
import wherehows.utils.ProcessorUtil;

import static wherehows.util.UrnUtil.*;
import static wherehows.utils.ProcessorUtil.*;


@Slf4j
public class MetadataInventoryProcessor extends KafkaMessageProcessor {

  private final Set<String> _whitelistActors;

  private final DatasetViewDao _datasetViewDao;

  public MetadataInventoryProcessor(Config config, DaoFactory daoFactory, String producerTopic,
      KafkaProducer<String, IndexedRecord> producer) {
    super(producerTopic, producer);

    _datasetViewDao = daoFactory.getDatasetViewDao();

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
      for (MetadataChangeEvent mce : processEvent(event)) {
        sendMessage(mce);
        log.info("set " + mce.datasetIdentifier + " removed");
      }
    } catch (Exception exception) {
      log.error("MIE Processor Error:", exception);
      log.error("Message content: {}", event.toString());
    }
  }

  public List<MetadataChangeEvent> processEvent(MetadataInventoryEvent event) throws Exception {
    final ChangeAuditStamp changeAuditStamp = event.changeAuditStamp;
    final String actorUrn = changeAuditStamp.actorUrn == null ? null : changeAuditStamp.actorUrn.toString();
    if (_whitelistActors != null && !_whitelistActors.contains(actorUrn)) {
      throw new UnauthorizedException("Actor " + actorUrn + " not in whitelist, skip processing");
    }

    final String platformUrn = event.dataPlatformUrn.toString();
    final String platform = getUrnEntity(platformUrn);
    final DataOrigin origin = event.dataOrigin;
    final String cluster = event.deployment.cluster.toString(); // if null cluster, throw exception here
    final String namespace = event.namespace.toString();

    log.info("Processing MIE for " + platform + " " + origin + " " + namespace);

    final List<Pattern> exclusions =
        event.exclusionPatterns.stream().map(s -> Pattern.compile(s.toString())).collect(Collectors.toList());

    final List<String> names = event.nativeNames.stream().map(CharSequence::toString).collect(Collectors.toList());
    log.debug("new datasets: " + names);

    final List<String> existingDatasets = _datasetViewDao.listFullNames(platform, origin.name(), cluster, namespace);
    log.debug("existing datasets: " + existingDatasets);

    // find removed datasets by diff
    return ProcessorUtil.listDiffWithExclusion(existingDatasets, names, exclusions).stream().map(datasetName -> {
      // send MCE to DELETE dataset
      DatasetIdentifier identifier = new DatasetIdentifier();
      identifier.dataPlatformUrn = platformUrn;
      identifier.dataOrigin = origin;
      identifier.nativeName = datasetName;

      return mceDelete(identifier, event.deployment, actorUrn);
    }).collect(Collectors.toList());
  }

  public FailedMetadataInventoryEvent newFailedEvent(MetadataInventoryEvent event, Throwable throwable) {
    FailedMetadataInventoryEvent failedEvent = new FailedMetadataInventoryEvent();
    failedEvent.time = System.currentTimeMillis();
    failedEvent.error = ExceptionUtils.getStackTrace(throwable);
    failedEvent.metadataInventoryEvent = event;
    return failedEvent;
  }
}
