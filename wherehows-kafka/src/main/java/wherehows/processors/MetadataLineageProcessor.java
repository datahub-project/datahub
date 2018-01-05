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
import com.linkedin.events.metadata.DatasetLineage;
import com.linkedin.events.metadata.DeploymentDetail;
import com.linkedin.events.metadata.FailedMetadataLineageEvent;
import com.linkedin.events.metadata.MetadataLineageEvent;
import com.linkedin.events.metadata.agent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import wherehows.dao.DaoFactory;
import wherehows.dao.table.LineageDao;


@Slf4j
public class MetadataLineageProcessor extends KafkaMessageProcessor {

  private final Config config = ConfigFactory.load();

  private final String whitelistStr = config.hasPath("whitelist.mle") ? config.getString("whitelist.mle") : "";

  private final Set<String> whitelistActors =
      StringUtils.isBlank(whitelistStr) ? null : new HashSet<>(Arrays.asList(whitelistStr.split(";")));

  private final LineageDao _lineageDao = DAO_FACTORY.getLineageDao();

  public MetadataLineageProcessor(DaoFactory daoFactory, String producerTopic,
      KafkaProducer<String, IndexedRecord> producer) {
    super(daoFactory, producerTopic, producer);
    log.info("MLE whitelist: " + whitelistActors);
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
      this.PRODUCER.send(new ProducerRecord(_producerTopic, newFailedEvent(event, exception)));
    }
  }

  private void processEvent(MetadataLineageEvent event) throws Exception {
    if (event.lineage == null || event.lineage.size() == 0) {
      throw new IllegalArgumentException("No Lineage info in record");
    }
    log.debug("MLE: " + event.lineage.toString());

    String actorUrn = getActorUrn(event);

    if (whitelistActors != null && !whitelistActors.contains(actorUrn)) {
      throw new RuntimeException("Actor " + actorUrn + " not in whitelist, skip processing");
    }

    List<DatasetLineage> lineages = event.lineage;
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
}
