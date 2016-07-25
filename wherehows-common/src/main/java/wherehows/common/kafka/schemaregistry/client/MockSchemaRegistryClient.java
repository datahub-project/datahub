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

package wherehows.common.kafka.schemaregistry.client;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import wherehows.common.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import wherehows.common.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Mock implementation of SchemaRegistryClient that can be used for tests. This version is NOT
 * thread safe. Schema data is stored in memory and is not persistent or shared across instances.
 */
public class MockSchemaRegistryClient implements SchemaRegistryClient {

  private String defaultCompatibility = "BACKWARD";
  private final Map<String, Map<Schema, String>> schemaCache;
  private final Map<String, Map<String, Schema>> idCache;
  private final Map<String, Map<Schema, Integer>> versionCache;
  private final Map<String, String> compatibilityCache;
  private final AtomicInteger ids;

  public MockSchemaRegistryClient() {
    schemaCache = new HashMap<String, Map<Schema, String>>();
    idCache = new HashMap<String, Map<String, Schema>>();
    versionCache = new HashMap<String, Map<Schema, Integer>>();
    compatibilityCache = new HashMap<String, String>();
    ids = new AtomicInteger(0);
    idCache.put(null, new HashMap<String, Schema>());
  }

  private String getIdFromRegistry(String subject, Schema schema) throws IOException {
    Map<String, Schema> idSchemaMap;
    if (idCache.containsKey(subject)) {
      idSchemaMap = idCache.get(subject);
      for (Map.Entry<String, Schema> entry: idSchemaMap.entrySet()) {
        if (entry.getValue().toString().equals(schema.toString())) {
          generateVersion(subject, schema);
          return entry.getKey();
        }
      }
    } else {
      idSchemaMap = new HashMap<String, Schema>();
    }
    String id = Integer.toString(ids.incrementAndGet());
    idSchemaMap.put(id, schema);
    idCache.put(subject, idSchemaMap);
    generateVersion(subject, schema);
    return id;
  }

  private void generateVersion(String subject, Schema schema) {
    ArrayList<Integer> versions = getAllVersions(subject);
    Map<Schema, Integer> schemaVersionMap;
    int currentVersion;
    if (versions.isEmpty()) {
      schemaVersionMap = new IdentityHashMap<Schema, Integer>();
      currentVersion = 1;
    } else {
      schemaVersionMap = versionCache.get(subject);
      currentVersion = versions.get(versions.size() - 1) + 1;
    }
    schemaVersionMap.put(schema, currentVersion);
    versionCache.put(subject, schemaVersionMap);
  }

  private ArrayList<Integer> getAllVersions(String subject) {
    ArrayList<Integer> versions = new ArrayList<Integer>();
    if (versionCache.containsKey(subject)) {
      versions.addAll(versionCache.get(subject).values());
      Collections.sort(versions);
    }
    return versions;
  }

  private Schema getSchemaBySubjectAndIdFromRegistry(String subject, String id) throws IOException {
    if (idCache.containsKey(subject)) {
      Map<String, Schema> idSchemaMap = idCache.get(subject);
      if (idSchemaMap.containsKey(id)) {
        return idSchemaMap.get(id);
      }
    }
    throw new IOException("Cannot get schema from schema registry!");
  }

  @Override
  public synchronized String register(String subject, Schema schema)
      throws IOException, RestClientException {
    Map<Schema, String> schemaIdMap;
    if (schemaCache.containsKey(subject)) {
      schemaIdMap = schemaCache.get(subject);
    } else {
      schemaIdMap = new IdentityHashMap<Schema, String>();
      schemaCache.put(subject, schemaIdMap);
    }

    if (schemaIdMap.containsKey(schema)) {
      return schemaIdMap.get(schema);
    } else {
      String id = getIdFromRegistry(subject, schema);
      schemaIdMap.put(schema, id);
      idCache.get(null).put(id, schema);
      return id;
    }
  }

  @Override
  public synchronized Schema getByID(String id) throws IOException, RestClientException {
    return getBySubjectAndID(null, id);
  }

  @Override
  public synchronized Schema getBySubjectAndID(String subject, String id)
      throws IOException, RestClientException {
    Map<String, Schema> idSchemaMap;
    if (idCache.containsKey(subject)) {
      idSchemaMap = idCache.get(subject);
    } else {
      idSchemaMap = new HashMap<String, Schema>();
      idCache.put(subject, idSchemaMap);
    }

    if (idSchemaMap.containsKey(id)) {
      return idSchemaMap.get(id);
    } else {
      Schema schema = getSchemaBySubjectAndIdFromRegistry(subject, id);
      idSchemaMap.put(id, schema);
      return schema;
    }
  }

   private int getLatestVersion(String subject)
      throws IOException, RestClientException {
    ArrayList<Integer> versions = getAllVersions(subject);
    if (versions.isEmpty()) {
      throw new IOException("No schema registered under subject!");
    } else {
      return versions.get(versions.size() - 1);
    }
  }

  @Override
  public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    int version = getLatestVersion(subject);
    String schemaString = null;
    Map<Schema, Integer> schemaVersionMap = versionCache.get(subject);
    for (Map.Entry<Schema, Integer> entry: schemaVersionMap.entrySet()) {
      if (entry.getValue() == version) {
        schemaString = entry.getKey().toString();
      }
    }
    String id = "-1";
    Map<String, Schema> idSchemaMap = idCache.get(subject);
    for (Map.Entry<String, Schema> entry: idSchemaMap.entrySet()) {
      if (entry.getValue().toString().equals(schemaString)) {
         id = entry.getKey();
      }
    }
    return new SchemaMetadata(id, version, schemaString);
  }

  @Override
  public synchronized int getVersion(String subject, Schema schema)
      throws IOException, RestClientException{
    if (versionCache.containsKey(subject)) {
      return versionCache.get(subject).get(schema);
    } else {
      throw new IOException("Cannot get version from schema registry!");
    }
  }

  @Override
  public boolean testCompatibility(String subject, Schema newSchema) throws IOException,
      RestClientException {
    SchemaMetadata latestSchemaMetadata = getLatestSchemaMetadata(subject);
    Schema latestSchema = getSchemaBySubjectAndIdFromRegistry(subject, latestSchemaMetadata.getId());
    String compatibility = compatibilityCache.get(subject);
    if (compatibility == null) {
      compatibility = defaultCompatibility;
    }

    AvroCompatibilityLevel compatibilityLevel = AvroCompatibilityLevel.forName(compatibility);
    if (compatibilityLevel == null) {
      return false;
    }

    return compatibilityLevel.compatibilityChecker.isCompatible(newSchema, latestSchema);
  }

  @Override
  public String updateCompatibility(String subject, String compatibility) throws IOException,
      RestClientException {
    if (subject == null) {
      defaultCompatibility = compatibility;
      return compatibility;
    }
    compatibilityCache.put(subject, compatibility);
    return compatibility;
  }

  @Override
  public String getCompatibility(String subject) throws IOException, RestClientException {
    String compatibility = compatibilityCache.get(subject);
    if (compatibility == null) {
      compatibility = defaultCompatibility;
    }
    return compatibility;
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    List<String> results = new ArrayList<>();
    results.addAll(this.schemaCache.keySet());
    Collections.sort(results, String.CASE_INSENSITIVE_ORDER);
    return results;
  }
}
