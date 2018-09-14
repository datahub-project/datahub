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

import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DatasetLineage;
import com.linkedin.events.metadata.JobExecution;
import com.linkedin.events.metadata.JobStatus;
import com.linkedin.events.metadata.MetadataLineageEvent;
import com.linkedin.events.metadata.agent;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import wherehows.ingestion.exceptions.SelfLineageException;
import wherehows.dao.DaoFactory;
import wherehows.dao.table.LineageDao;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;
import static wherehows.ingestion.util.ProcessorUtilTest.*;


public class MetadataLineageProcessorTest {

  private MetadataLineageProcessor _processor;

  @BeforeTest
  public void setup() {
    DaoFactory mockDaoFactory = mock(DaoFactory.class);
    when(mockDaoFactory.getLineageDao()).thenReturn(mock(LineageDao.class));

    _processor = new MetadataLineageProcessor(new Properties(), mockDaoFactory, "topic", mock(KafkaProducer.class));
  }

  @Test
  public void testDedupeDatasets() {
    DatasetIdentifier ds1 = makeDataset("foo");
    DatasetIdentifier ds2 = makeDataset("foo");
    DatasetIdentifier ds3 = makeDataset("bar");
    DatasetIdentifier ds4 = makeDataset("bar");
    DatasetIdentifier ds5 = makeDataset("bar");
    DatasetLineage lineage = new DatasetLineage();
    lineage.sourceDataset = Arrays.asList(ds1, ds2);
    lineage.destinationDataset = Arrays.asList(ds3, ds4, ds5);

    _processor.dedupeAndValidateLineage(lineage);
    assertEquals(lineage.sourceDataset, Collections.singletonList(ds1));
    assertEquals(lineage.destinationDataset, Collections.singletonList(ds3));
  }

  @Test(expectedExceptions = SelfLineageException.class)
  public void testSelfLineageValid() throws Exception {
    DatasetIdentifier src1 = makeDataset("foo");
    DatasetIdentifier src2 = makeDataset("bar");
    DatasetIdentifier dest = makeDataset("foo");
    MetadataLineageEvent mle =
        newMetadataLineageEvent(Arrays.asList(src1, src2), Arrays.asList(dest), JobStatus.SUCCEEDED);

    _processor.processEvent(mle);
  }

  // JobStatus RUNNING invalid MLE event
  @Test
  public void testLineageInvalid() {
    DatasetIdentifier src1 = makeDataset("foo");
    DatasetIdentifier src2 = makeDataset("bar");
    DatasetIdentifier dest = makeDataset("foo");
    MetadataLineageEvent mle =
        newMetadataLineageEvent(Arrays.asList(src1, src2), Arrays.asList(dest), JobStatus.RUNNING);

    try {
      _processor.processEvent(mle);
    } catch (Exception ex) {
      assertTrue(ex instanceof UnsupportedOperationException);
    }
  }

  private MetadataLineageEvent newMetadataLineageEvent(List<DatasetIdentifier> sourceDatasets,
      List<DatasetIdentifier> destinationDataset, JobStatus jobStatus) {
    MetadataLineageEvent mle = new MetadataLineageEvent();

    mle.type = agent.UNUSED;
    mle.changeAuditStamp = new ChangeAuditStamp();
    mle.changeAuditStamp.actorUrn = "tester";
    mle.jobExecution = new JobExecution();
    mle.jobExecution.status = jobStatus;

    DatasetLineage lineage = new DatasetLineage();
    lineage.sourceDataset = sourceDatasets;
    lineage.destinationDataset = destinationDataset;
    mle.lineage = Collections.singletonList(lineage);

    return mle;
  }
}
