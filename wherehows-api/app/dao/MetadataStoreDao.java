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
package dao;

import com.linkedin.dataset.SchemaField;
import com.linkedin.dataset.SchemaFieldArray;
import com.linkedin.dataset.SchemaMetadata;
import com.linkedin.restli.client.RestClient;
import java.util.ArrayList;
import java.util.List;
import metastore.client.RestliClient;
import metastore.client.Schemas;
import models.DatasetColumn;
import play.Play;

import static metastore.util.UrnUtil.*;


public class MetadataStoreDao {

  private static final String MetadataStoreURL =
      Play.application().configuration().getString("wherehows.restli.server.url");

  private static final RestliClient metaStore = new RestliClient(MetadataStoreURL);
  private static final RestClient _client = metaStore.getClient();

  private static final Schemas _schemas = new Schemas(_client);


  public static SchemaMetadata getLatestSchemaByWhUrn(String urn) throws Exception {
    String[] urnParts = splitWhUrn(urn);
    return _schemas.getLatestSchemaByDataset(urnParts[0], urnParts[1], "PROD");
  }

  public static List<DatasetColumn> datasetColumnsMapper(SchemaFieldArray fields) {
    List<DatasetColumn> columns = new ArrayList<>();
    for (SchemaField field : fields) {
      DatasetColumn col = new DatasetColumn();
      col.fieldName = field.getFieldPath();
      col.fullFieldPath = field.getFieldPath();
      col.dataType = field.hasNativeDataType() ? field.getNativeDataType() : "";
      col.comment = field.hasDescription() ? field.getDescription() : "";
      columns.add(col);
    }
    return columns;
  }
}
