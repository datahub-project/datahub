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
package wherehows.ingestion.processors;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DatasetLineage;
import com.linkedin.events.metadata.FailedMetadataLineageEvent;
import com.linkedin.events.metadata.JobStatus;
import com.linkedin.events.metadata.MetadataLineageEvent;
import com.linkedin.events.metadata.agent;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.ingestion.exceptions.SelfLineageException;
import wherehows.ingestion.exceptions.UnauthorizedException;
import wherehows.dao.DaoFactory;
import wherehows.dao.table.LineageDao;
import wherehows.ingestion.utils.ProcessorUtil;

import static wherehows.ingestion.utils.ProcessorUtil.*;


@Slf4j
public class MetadataLineageProcessor extends KafkaMessageProcessor {

  private final LineageDao _lineageDao;

  private final Set<String> _whitelistActors;

  public MetadataLineageProcessor(@Nonnull Properties config, @Nonnull DaoFactory daoFactory,
      @Nonnull String producerTopic, @Nonnull KafkaProducer<String, IndexedRecord> producer) {
    super(config, daoFactory, producerTopic, producer);

    this._lineageDao = _daoFactory.getLineageDao();

    _whitelistActors = ProcessorUtil.getWhitelistedActors(_config, "whitelist.mle");
    log.info("MLE whitelist: " + _whitelistActors);
  }

  /**
   * Process a MetadataLineageEvent record
   * @param indexedRecord IndexedRecord
   */
  public void process(IndexedRecord indexedRecord) {
    if (indexedRecord == null || indexedRecord.getClass() != MetadataLineageEvent.class) {
      throw new IllegalArgumentException("Invalid record");
    }

    log.debug("Processing Metadata Lineage Event record. ");

    MetadataLineageEvent event = (MetadataLineageEvent) indexedRecord;
    try {
      processEvent(event);
    } catch (Exception ex) {
      if (ex instanceof SelfLineageException || ex.toString().contains("Response status 404")) {
        log.warn(ex.toString());
      } else {
        log.error("MLE Processor Error:", ex);
        log.error("Message content: {}", event.toString());
      }
      sendMessage(newFailedEvent(event, ex));
    }
  }

  public void processEvent(MetadataLineageEvent event) throws Exception {
    if (event.lineage == null || event.lineage.size() == 0) {
      throw new IllegalArgumentException("No Lineage info in record");
    }
    log.debug("MLE: " + event.lineage.toString());

    if (event.jobExecution.status != JobStatus.SUCCEEDED) {
      log.info("Discard: job status " + event.jobExecution.status.name());
      return; // discard message if job status is not SUCCEEDED
    }

    String actorUrn = getActorUrn(event);
    if (_whitelistActors != null && !_whitelistActors.contains(actorUrn)) {
      throw new UnauthorizedException("Actor " + actorUrn + " not in whitelist, skip processing");
    }

    List<DatasetLineage> lineages = event.lineage;
    for (DatasetLineage lineage : lineages) {
      dedupeAndValidateLineage(lineage);
    }

    // create lineage
    _lineageDao.createLineages(actorUrn, lineages, event.deploymentDetail);
  }

  /**
   * Get actor Urn string from app type or change audit stamp
   */
  private String getActorUrn(MetadataLineageEvent event) {
    // use actorUrn in ChangeAuditStamp first
    ChangeAuditStamp auditStamp = event.changeAuditStamp;
    if (auditStamp != null && auditStamp.actorUrn != null) {
      return auditStamp.actorUrn.toString();
    }

    // use app type as fallback
    if (event.type != agent.UNUSED) {
      return "urn:li:multiProduct:" + event.type.name().toLowerCase();
    }
    throw new IllegalArgumentException("Require ChangeAuditStamp actorUrn");
  }

  public FailedMetadataLineageEvent newFailedEvent(MetadataLineageEvent event, Throwable throwable) {
    FailedMetadataLineageEvent failedEvent = new FailedMetadataLineageEvent();
    failedEvent.time = System.currentTimeMillis();
    failedEvent.error = ExceptionUtils.getStackTrace(throwable);
    failedEvent.metadataLineageEvent = event;
    return failedEvent;
  }

  @VisibleForTesting
  void dedupeAndValidateLineage(DatasetLineage lineage) {
    lineage.sourceDataset = dedupeDatasets(lineage.sourceDataset);
    lineage.destinationDataset = dedupeDatasets(lineage.destinationDataset);

    // check intersection of source and destination
    List<DatasetIdentifier> intersection = new ArrayList<>(lineage.sourceDataset);
    intersection.retainAll(lineage.destinationDataset);
    if (intersection.size() > 0) {
      throw new SelfLineageException("Source & destination datasets shouldn't overlap");
    }
  }
}
