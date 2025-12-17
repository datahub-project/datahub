/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import static org.junit.jupiter.api.Assertions.*;

import io.openlineage.spark.agent.lifecycle.plan.column.SchemaHistoryTracker.SchemaMismatch;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaHistoryTrackerTest {

  private SchemaHistoryTracker tracker;

  @BeforeEach
  void setUp() {
    tracker = new SchemaHistoryTracker();
  }

  @Test
  void testRecordWrite() {
    List<String> schema = Arrays.asList("id", "name");
    tracker.recordWrite("dataset1", schema);

    assertEquals(1, tracker.getTrackedDatasetCount());
  }

  @Test
  void testRecordRead() {
    List<String> schema = Arrays.asList("id", "name");
    tracker.recordRead("dataset1", schema);

    assertEquals(1, tracker.getTrackedDatasetCount());
  }

  @Test
  void testDetectMismatch_whenSchemasMatch() {
    List<String> schema = Arrays.asList("id", "name");

    tracker.recordWrite("dataset1", schema);
    tracker.recordRead("dataset1", schema);

    Optional<SchemaMismatch> mismatch = tracker.detectMismatch("dataset1");
    assertFalse(mismatch.isPresent());
  }

  @Test
  void testDetectMismatch_whenSchemasDiffer() {
    List<String> writeSchema = Arrays.asList("id", "name");
    List<String> readSchema = Arrays.asList("cust_id", "cust_name");

    tracker.recordWrite("dataset1", writeSchema);
    tracker.recordRead("dataset1", readSchema);

    Optional<SchemaMismatch> mismatch = tracker.detectMismatch("dataset1");
    assertTrue(mismatch.isPresent());

    SchemaMismatch detected = mismatch.get();
    assertEquals("dataset1", detected.getDataset());
    assertEquals(writeSchema, detected.getActualSchema());
    assertEquals(readSchema, detected.getImposedSchema());
  }

  @Test
  void testDetectMismatch_whenSchemasDifferButDifferentLengths() {
    List<String> writeSchema = Arrays.asList("id", "name", "age");
    List<String> readSchema = Arrays.asList("cust_id", "cust_name");

    tracker.recordWrite("dataset1", writeSchema);
    tracker.recordRead("dataset1", readSchema);

    Optional<SchemaMismatch> mismatch = tracker.detectMismatch("dataset1");
    // Should not detect mismatch because column counts differ
    assertFalse(mismatch.isPresent());
  }

  @Test
  void testDetectMismatch_whenOnlyWriteExists() {
    List<String> schema = Arrays.asList("id", "name");
    tracker.recordWrite("dataset1", schema);

    Optional<SchemaMismatch> mismatch = tracker.detectMismatch("dataset1");
    assertFalse(mismatch.isPresent());
  }

  @Test
  void testDetectMismatch_whenOnlyReadExists() {
    List<String> schema = Arrays.asList("id", "name");
    tracker.recordRead("dataset1", schema);

    Optional<SchemaMismatch> mismatch = tracker.detectMismatch("dataset1");
    assertFalse(mismatch.isPresent());
  }

  @Test
  void testDetectMismatch_nonExistentDataset() {
    Optional<SchemaMismatch> mismatch = tracker.detectMismatch("nonexistent");
    assertFalse(mismatch.isPresent());
  }

  @Test
  void testCreatePositionalMapping() {
    List<String> actualSchema = Arrays.asList("id", "name", "age");
    List<String> imposedSchema = Arrays.asList("cust_id", "cust_name", "cust_age");

    SchemaMismatch mismatch = new SchemaMismatch("dataset1", actualSchema, imposedSchema);
    Map<String, String> mapping = mismatch.createPositionalMapping();

    assertEquals(3, mapping.size());
    assertEquals("id", mapping.get("cust_id"));
    assertEquals("name", mapping.get("cust_name"));
    assertEquals("age", mapping.get("cust_age"));
  }

  @Test
  void testCreatePositionalMapping_whenLengthsDiffer() {
    List<String> actualSchema = Arrays.asList("id", "name");
    List<String> imposedSchema = Arrays.asList("cust_id", "cust_name", "cust_age");

    SchemaMismatch mismatch = new SchemaMismatch("dataset1", actualSchema, imposedSchema);
    Map<String, String> mapping = mismatch.createPositionalMapping();

    assertTrue(mapping.isEmpty());
  }

  @Test
  void testClear() {
    tracker.recordWrite("dataset1", Arrays.asList("id", "name"));
    tracker.recordWrite("dataset2", Arrays.asList("col1", "col2"));

    assertEquals(2, tracker.getTrackedDatasetCount());

    tracker.clear();

    assertEquals(0, tracker.getTrackedDatasetCount());
  }

  @Test
  void testSnapshotCleanup_byAge() throws InterruptedException {
    // Create tracker with very short max age
    SchemaHistoryTracker shortAgeTracker = new SchemaHistoryTracker(100L, 10);

    shortAgeTracker.recordWrite("dataset1", Arrays.asList("id", "name"));

    // Wait longer than max age
    Thread.sleep(150);

    // Record another operation to trigger cleanup
    shortAgeTracker.recordRead("dataset1", Arrays.asList("cust_id", "cust_name"));

    // The write snapshot should have been cleaned up, so no mismatch
    Optional<SchemaMismatch> mismatch = shortAgeTracker.detectMismatch("dataset1");
    assertFalse(mismatch.isPresent());
  }

  @Test
  void testSnapshotCleanup_byCount() {
    // Create tracker with max 2 snapshots
    SchemaHistoryTracker limitedTracker = new SchemaHistoryTracker(3600000L, 2);

    // Add 3 snapshots
    limitedTracker.recordWrite("dataset1", Arrays.asList("id", "name"));
    limitedTracker.recordRead("dataset1", Arrays.asList("cust_id", "cust_name"));
    limitedTracker.recordWrite("dataset1", Arrays.asList("user_id", "user_name"));

    // Should still track the dataset
    assertEquals(1, limitedTracker.getTrackedDatasetCount());
  }

  @Test
  void testMultipleDatasets() {
    tracker.recordWrite("dataset1", Arrays.asList("id", "name"));
    tracker.recordRead("dataset1", Arrays.asList("cust_id", "cust_name"));

    tracker.recordWrite("dataset2", Arrays.asList("col1", "col2"));
    tracker.recordRead("dataset2", Arrays.asList("field1", "field2"));

    assertEquals(2, tracker.getTrackedDatasetCount());

    Optional<SchemaMismatch> mismatch1 = tracker.detectMismatch("dataset1");
    Optional<SchemaMismatch> mismatch2 = tracker.detectMismatch("dataset2");

    assertTrue(mismatch1.isPresent());
    assertTrue(mismatch2.isPresent());
  }
}
