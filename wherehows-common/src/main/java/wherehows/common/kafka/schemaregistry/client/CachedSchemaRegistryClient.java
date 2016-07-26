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

import wherehows.common.kafka.schemaregistry.client.rest.RestService;
import wherehows.common.kafka.schemaregistry.client.rest.entities.Config;
import wherehows.common.kafka.schemaregistry.client.rest.entities.SchemaString;
import wherehows.common.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import wherehows.common.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class CachedSchemaRegistryClient implements SchemaRegistryClient {

  private final RestService restService;
  private final int identityMapCapacity;
  private final Map<String, Map<Schema, String>> schemaCache;
  private final Map<String, Map<String, Schema>> idCache;
  private final Map<String, Map<Schema, Integer>> versionCache;

  private static final int defaultIdentityMapCapacity = 1000;

  public CachedSchemaRegistryClient(String baseUrl) {
    this(baseUrl, defaultIdentityMapCapacity);
  }

  public CachedSchemaRegistryClient(String baseUrl, int identityMapCapacity) {
    this(new RestService(baseUrl), identityMapCapacity);
  }

  public CachedSchemaRegistryClient(List<String> baseUrls, int identityMapCapacity) {
    this(new RestService(baseUrls), identityMapCapacity);
  }

  public CachedSchemaRegistryClient(RestService restService, int identityMapCapacity) {
    this.identityMapCapacity = identityMapCapacity;
    this.schemaCache = new HashMap<String, Map<Schema, String>>();
    this.idCache = new HashMap<String, Map<String, Schema>>();
    this.versionCache = new HashMap<String, Map<Schema, Integer>>();
    this.restService = restService;
    this.idCache.put(null, new HashMap<String, Schema>());
  }

  private String registerAndGetId(String subject, Schema schema)
      throws IOException, RestClientException {
    return restService.registerSchema(schema.toString(), subject);
  }

  private Schema getSchemaByIdFromRegistry(String id) throws IOException, RestClientException {
    SchemaString restSchema = restService.getId(id);
    return new Schema.Parser().parse(restSchema.getSchemaString());
  }

  private int getVersionFromRegistry(String subject, Schema schema)
      throws IOException, RestClientException{
    wherehows.common.kafka.schemaregistry.client.rest.entities.Schema response =
        restService.lookUpSubjectVersion(schema.toString(), subject);
    return response.getVersion();
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
      if (schemaIdMap.size() >= identityMapCapacity) {
        throw new IllegalStateException("Too many schema objects created for " + subject + "!");
      }
      String id = registerAndGetId(subject, schema);
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
      Schema schema = getSchemaByIdFromRegistry(id);
      idSchemaMap.put(id, schema);
      return schema;
    }
  }

  @Override
  public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    wherehows.common.kafka.schemaregistry.client.rest.entities.Schema response
     = restService.getLatestVersion(subject);
    String id = response.getId();
    int version = response.getVersion();
    String schema = response.getSchema();
    return new SchemaMetadata(id, version, schema);
  }

  @Override
  public synchronized int getVersion(String subject, Schema schema)
      throws IOException, RestClientException{
    Map<Schema, Integer> schemaVersionMap;
    if (versionCache.containsKey(subject)) {
      schemaVersionMap = versionCache.get(subject);
    } else {
      schemaVersionMap = new IdentityHashMap<Schema, Integer>();
      versionCache.put(subject, schemaVersionMap);
    }

    if (schemaVersionMap.containsKey(schema)) {
      return schemaVersionMap.get(schema);
    }  else {
      if (schemaVersionMap.size() >= identityMapCapacity) {
        throw new IllegalStateException("Too many schema objects created for " + subject + "!");
      }
      int version = getVersionFromRegistry(subject, schema);
      schemaVersionMap.put(schema, version);
      return version;
    }
  }

  @Override
  public boolean testCompatibility(String subject, Schema schema) throws IOException, RestClientException {
    return restService.testCompatibility(schema.toString(), subject, "latest");
  }

  @Override
  public String updateCompatibility(String subject, String compatibility) throws IOException, RestClientException {
    ConfigUpdateRequest response = restService.updateCompatibility(compatibility, subject);
    return response.getCompatibilityLevel();
  }

  @Override
  public String getCompatibility(String subject) throws IOException, RestClientException {
    Config response = restService.getConfig(subject);
    return response.getCompatibilityLevel();
  }

  @Override
  public Collection<String> getAllSubjects() throws IOException, RestClientException {
    return restService.getAllSubjects();
  }

}
