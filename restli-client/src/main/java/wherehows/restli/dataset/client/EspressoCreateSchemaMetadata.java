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

package wherehows.restli.dataset.client;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.ForeignKeySpecMap;
import com.linkedin.dataset.EspressoSchema;
import com.linkedin.dataset.SchemaFieldArray;
import com.linkedin.dataset.SchemaMetadata;
import com.linkedin.dataset.SchemaMetadataKey;
import com.linkedin.dataset.SchemaMetadataRequestBuilders;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


import static wherehows.restli.dataset.client.EspressoCreateDataSet.toDataPlatformUrn;
import static wherehows.restli.dataset.client.EspressoCreateDataSet.toDatasetUrn;

import java.net.URISyntaxException;

public class EspressoCreateSchemaMetadata {

  public static void create(RestClient _restClient, String iSchemaName, String iTableSchemaData, String iDocumentSchemaData, String iDataSetName, String iUrn)
      throws Exception {

    DatasetUrnArray datasetUrns = new DatasetUrnArray();
    datasetUrns.add(toDatasetUrn(iDataSetName, toDataPlatformUrn("espresso"), FabricType.PROD));

    EspressoSchema espressoSchema = new EspressoSchema().setTableSchema(iTableSchemaData).setDocumentSchema(iDocumentSchemaData);
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setEspressoSchema(espressoSchema);

    SchemaMetadata newSchemaMetadata = new SchemaMetadata().setSchemaName(iSchemaName)
        .setPlatform(toDataPlatformUrn("espresso"))
        .setPermissibleDatasets(datasetUrns)
        .setPlatformSchema(platformSchema)
        .setCreated(new AuditStamp().setActor(new Urn(iUrn)))
        .setLastModified(new AuditStamp().setActor(new Urn(iUrn)))
        .setPlatformSchemaVersion("version-101")
        .setFields( new SchemaFieldArray())
        .setPrimaryKeys(new StringArray())
        .setForeignKeysSpecs(new ForeignKeySpecMap());

    try {
      IdResponse<ComplexResourceKey<SchemaMetadataKey, EmptyRecord>> response =
          _restClient.sendRequest(_schemaMetadataBuilders.create().input(newSchemaMetadata).actorParam(new Urn(iUrn)).build()).getResponseEntity();

      System.out.println(response.getId().getKey());
    } catch (Exception e) {
      System.out.println(_schemaMetadataBuilders.create().input(newSchemaMetadata).build());
      System.out.println(e.fillInStackTrace());
    }

  }

  private static final SchemaMetadataRequestBuilders _schemaMetadataBuilders = new SchemaMetadataRequestBuilders();
}
