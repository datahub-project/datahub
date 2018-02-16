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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.DatasetLineage;
import com.linkedin.events.metadata.DeploymentDetail;
import com.linkedin.events.metadata.FailedMetadataLineageEvent;
import com.linkedin.events.metadata.MetadataLineageEvent;
import com.linkedin.events.metadata.agent;
import com.typesafe.config.Config;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import wherehows.dao.DaoFactory;
import wherehows.dao.table.LineageDao;
import wherehows.exceptions.SelfLineageException;
import wherehows.exceptions.UnauthorizedException;


@Slf4j
public class MetadataLineageProcessor extends KafkaMessageProcessor {

  private final LineageDao _lineageDao;

  private final Set<String> _whitelistActors;

  public MetadataLineageProcessor(Config config, DaoFactory daoFactory, String producerTopic,
      KafkaProducer<String, IndexedRecord> producer) {
    super(producerTopic, producer);
    this._lineageDao = daoFactory.getLineageDao();

    _whitelistActors = getWhitelistedActors(config, "whitelist.mle");
    log.info("MLE whitelist: " + _whitelistActors);
  }

  /**
   * Process a MetadataLineageEvent record
   * @param indexedRecord IndexedRecord
   * @throws Exception
   */
  public void process(IndexedRecord indexedRecord) {
    if (indexedRecord == null || indexedRecord.getClass() != MetadataLineageEvent.class) {
      throw new IllegalArgumentException("Invalid record");
    }

    log.debug("Processing Metadata Lineage Event record. ");

    MetadataLineageEvent event = (MetadataLineageEvent) indexedRecord;
    try {
      processEvent(event);
    } catch (Exception exception) {
      log.error("MLE Processor Error:", exception);
      log.error("Message content: {}", event.toString());
      sendMessage(newFailedEvent(event, exception));
    }
  }

  @VisibleForTesting
  void processEvent(MetadataLineageEvent event) throws Exception {
    if (event.lineage == null || event.lineage.size() == 0) {
      throw new IllegalArgumentException("No Lineage info in record");
    }
    log.debug("MLE: " + event.lineage.toString());

    String actorUrn = getActorUrn(event);
    if (_whitelistActors != null && !_whitelistActors.contains(actorUrn)) {
      throw new UnauthorizedException("Actor " + actorUrn + " not in whitelist, skip processing");
    }

    List<DatasetLineage> lineages = event.lineage;
    validateLineages(lineages);

    DeploymentDetail deployments = event.deploymentDetail;

    // create lineage
    _lineageDao.createLineages(actorUrn, lineages, deployments);
  }

  /**
   * Get actor Urn string from app type or change audit stamp
   */
  private String getActorUrn(MetadataLineageEvent event) {
    // use app type first
    if (event.type != agent.UNUSED) {
      return "urn:li:multiProduct:" + event.type.name().toLowerCase();
    }

    // if app type = UNUSED, use actorUrn in ChangeAuditStamp
    ChangeAuditStamp auditStamp = event.changeAuditStamp;
    if (auditStamp == null || auditStamp.actorUrn == null) {
      throw new IllegalArgumentException("Requires ChangeAuditStamp actorUrn if MLE agent is UNUSED");
    }
    return auditStamp.actorUrn.toString();
  }

  private FailedMetadataLineageEvent newFailedEvent(MetadataLineageEvent event, Throwable throwable) {
    FailedMetadataLineageEvent failedEvent = new FailedMetadataLineageEvent();
    failedEvent.time = System.currentTimeMillis();
    failedEvent.error = ExceptionUtils.getStackTrace(throwable);
    failedEvent.metadataLineageEvent = event;
    return failedEvent;
  }

  private void validateLineages(List<DatasetLineage> lineages) {
    for (DatasetLineage lineage : lineages) {
      if (Sets.intersection(new HashSet(lineage.sourceDataset), new HashSet(lineage.destinationDataset)).size() > 0) {
        throw new SelfLineageException("Source & destination datasets shouldn't overlap");
      }
    }
  }
}
