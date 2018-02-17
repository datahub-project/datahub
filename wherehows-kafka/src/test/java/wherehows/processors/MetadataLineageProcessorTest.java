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

import com.google.common.collect.ImmutableList;
import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.DataOrigin;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DatasetLineage;
import com.linkedin.events.metadata.MetadataLineageEvent;
import com.linkedin.events.metadata.agent;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import wherehows.dao.DaoFactory;
import wherehows.dao.table.LineageDao;
import wherehows.exceptions.SelfLineageException;

import static org.mockito.Mockito.*;


public class MetadataLineageProcessorTest {

  private Config _mockConfig;
  private DaoFactory _mockDaoFactory;
  private LineageDao _mockLineageDao;
  private MetadataLineageProcessor _processor;
  private KafkaProducer<String, IndexedRecord> _mockProducer;

  private static final String TOPIC = "testTopic";

  @BeforeTest
  public void setup() {
    _mockConfig = mock(Config.class);
    _mockLineageDao = mock(LineageDao.class);
    _mockDaoFactory = mock(DaoFactory.class);
    when(_mockDaoFactory.getLineageDao()).thenReturn(_mockLineageDao);
    _mockProducer = mock(KafkaProducer.class);
  }

  @Test(expectedExceptions = SelfLineageException.class)
  public void testSelfLineage() throws Exception {
    when(_mockConfig.hasPath("whitelist.mle")).thenReturn(false);
    _processor = new MetadataLineageProcessor(_mockConfig, _mockDaoFactory, TOPIC, _mockProducer);
    DatasetIdentifier src1 = newDatasetIdentifier("oracle", "foo", DataOrigin.DEV);
    DatasetIdentifier src2 = newDatasetIdentifier("oracle", "bar", DataOrigin.DEV);
    DatasetIdentifier dest = newDatasetIdentifier("oracle", "foo", DataOrigin.DEV);
    MetadataLineageEvent mle = newMetadataLineageEvent("foo", ImmutableList.of(src1, src2), ImmutableList.of(dest));

    _processor.processEvent(mle);
  }

  private MetadataLineageEvent newMetadataLineageEvent(String actorUrn, List<DatasetIdentifier> sourceDatasets,
      List<DatasetIdentifier> destinationDataset) {
    MetadataLineageEvent mle = new MetadataLineageEvent();

    mle.type = agent.UNUSED;

    mle.changeAuditStamp = new ChangeAuditStamp();
    mle.changeAuditStamp.actorUrn = actorUrn;

    DatasetLineage lineage = new DatasetLineage();
    lineage.sourceDataset = sourceDatasets;
    lineage.destinationDataset = destinationDataset;
    mle.lineage = Collections.singletonList(lineage);

    return mle;
  }

  private DatasetIdentifier newDatasetIdentifier(String platform, String name, DataOrigin dataOrigin) {
    DatasetIdentifier datasetIdentifier = new DatasetIdentifier();
    datasetIdentifier.dataPlatformUrn = "urn:li:dataPlatform:" + platform;
    datasetIdentifier.nativeName = name;
    datasetIdentifier.dataOrigin = dataOrigin;
    return datasetIdentifier;
  }
}
