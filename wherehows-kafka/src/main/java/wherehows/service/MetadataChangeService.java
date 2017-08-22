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
package wherehows.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import wherehows.common.utils.StringUtil;
import wherehows.dao.DatasetSchemaInfoDao;
import wherehows.dao.DictDatasetDao;
import wherehows.dao.DictFieldDetailDao;
import wherehows.models.DatasetFieldSchema;
import wherehows.models.DatasetSchemaInfo;
import wherehows.models.DictDataset;
import wherehows.models.DictFieldDetail;
import wherehows.util.Urn;


@Slf4j
@RequiredArgsConstructor
public class MetadataChangeService {

  private final DictDatasetDao dictDatasetDao;
  private final DictFieldDetailDao dictFieldDetailDao;
  private final DatasetSchemaInfoDao datasetSchemaInfoDao;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String UrnRegex = "urn:li:dataset:\\(urn:li:dataPlatform:(\\w*),\\s?([^\\s]*),\\s?(\\w*)\\)";

  private static Pattern UrnPattern = Pattern.compile(UrnRegex);

  private String urnToUri(String urn) {
    // from urn:li:dataset:(urn:li:dataPlatform:p, nativeName, dataOrigin) to p:///nativeName
    Matcher m = UrnPattern.matcher(urn);
    if (m.matches()) {
      String platform = m.group(1) + "://";
      String datasetName = m.group(2).replace(".", "/");
      return platform + (datasetName.startsWith("/") ? "" : "/") + datasetName;
    }
    return null;
  }

  private Object[] findIdAndUrn(Integer datasetId) throws SQLException {
    String urn = null;
    if (datasetId != null) {
      DictDataset dataset = dictDatasetDao.findById(datasetId);
      if (dataset != null) {
        urn = dataset.getUrn();
      }
    }
    return new Object[]{datasetId, urn};
  }

  private Object[] findIdAndUrn(String urn) throws SQLException {
    Integer datasetId = null;
    if (urn != null) {
      DictDataset dataset = dictDatasetDao.findByUrn(urn);
      if (dataset != null) {
        datasetId = dataset.getRefDatasetId();
      }
    }
    return new Object[]{datasetId, urn};
  }

  private Object[] findDataset(JsonNode root) {
    // use dataset id to find dataset first
    final JsonNode idNode = root.path("datasetId");
    if (!idNode.isMissingNode() && !idNode.isNull()) {
      try {
        final Object[] idUrn = findIdAndUrn(idNode.asInt());
        if (idUrn[0] != null && idUrn[1] != null) {
          return idUrn;
        }
      } catch (Exception ex) {
      }
    }

    // use dataset uri to find dataset
    final JsonNode properties = root.path("datasetProperties");
    if (!properties.isMissingNode() && !properties.isNull()) {
      final JsonNode uri = properties.path("uri");
      if (!uri.isMissingNode() && !uri.isNull()) {
        try {
          final Object[] idUrn = findIdAndUrn(uri.asText());
          if (idUrn[0] != null && idUrn[1] != null) {
            return idUrn;
          }
        } catch (Exception ex) {
        }
      }
    }

    // use dataset urn to find dataset
    final JsonNode urnNode = root.path("urn");
    if (!urnNode.isMissingNode() && !urnNode.isNull()) {
      try {
        final Object[] idUrn = findIdAndUrn(urnToUri(urnNode.asText()));
        if (idUrn[0] != null && idUrn[1] != null) {
          return idUrn;
        }
      } catch (Exception ex) {
      }
    }

    return new Object[]{null, null};
  }

  public void updateDatasetSchema(JsonNode root) throws Exception {
    final JsonNode schema = root.path("schema");
    if (schema.isMissingNode() || schema.isNull()) {
      throw new IllegalArgumentException("Dataset schema update fail, missing necessary fields: " + root.toString());
    }

    Integer datasetId = 0;
    String urn = null;
    final Object[] idUrn = findDataset(root);
    if (idUrn[0] != null && idUrn[1] != null) {
      datasetId = (Integer) idUrn[0];
      urn = (String) idUrn[1];
    } else if (root.get("datasetProperties") != null && root.get("datasetProperties").get("uri") != null) {
      urn = root.path("datasetProperties").path("uri").asText();
    } else if (root.get("urn") != null) {
      urn = urnToUri(root.get("urn").asText());
    } else {
      log.info("Can't identify dataset URN, abort process.");
      return;
    }

    DatasetSchemaInfo schemaInfo = OBJECT_MAPPER.convertValue(schema, DatasetSchemaInfo.class);
    schemaInfo.setDatasetId(datasetId);
    schemaInfo.setDatasetUrn(urn);
    //TODO need modify time type to have a correct timestamp
    schemaInfo.setModifiedTime(0);

    // insert dataset and get ID if necessary
    if (datasetId == 0) {
      DictDataset dataset = new DictDataset();
      dataset.setUrn(urn);
      dataset.setSourceCreatedTime(0);
      //TODO noticed DB doesn't have DatasetOriginalSchemaRecord table, so i just put the whole info in schema and schema type.
      dataset.setSchema(schemaInfo.getOriginalSchema());
      dataset.setSchemaType(schemaInfo.getOriginalSchema());
      dataset.setFields((String) StringUtil.objectToJsonString(schemaInfo.getFieldSchema()));
      dataset.setSource("API");
      dataset.setIsActive(true);

      Urn urnType = new Urn(urn);
      dataset.setDatasetType(urnType.datasetType);
      String[] urnPaths = urnType.abstractObjectName.split("/");
      dataset.setName(urnPaths[urnPaths.length - 1]);

      // DB insert

      datasetId = dictDatasetDao.findByUrn(urn).getId();
      schemaInfo.setDatasetId(datasetId);
    }// if dataset already exist in dict_dataset, update info
    else {
      DictDataset dataset = dictDatasetDao.findById(datasetId);
      //TODO noticed DB doesn't have DatasetOriginalSchemaRecord table, so i just put the whole info in schema and schema type.
      dataset.setSchema(schemaInfo.getOriginalSchema());
      dataset.setSchemaType(schemaInfo.getOriginalSchema());
      dataset.setFields(schemaInfo.getFieldSchema());
      dataset.setSource("api");

      dictDatasetDao.update(dataset);
    }

    List<DictFieldDetail> dictFieldDetails = dictFieldDetailDao.findById(datasetId);

    // NOTE:  removed merge old info step since there's no information need to merge to new table
    //  namespace, commentIds, defaultCommentId, isPartitioned, isIndexed

    // remove old info then insert new info

    dictFieldDetailDao.deleteByDatasetId(datasetId);
    List<DatasetFieldSchema> fieldSchemas = OBJECT_MAPPER.readValue(schemaInfo.getFieldSchema(), new TypeReference<List<DatasetFieldSchema>>(){});
    if (fieldSchemas != null && fieldSchemas.size() > 0) {
      for (DatasetFieldSchema field : fieldSchemas) {
        dictFieldDetailDao.update(field);
      }
    }
    datasetSchemaInfoDao.update(schemaInfo);
  }

  //TODO Complete the rest when we need to use
  public void updateDatasetOwner(JsonNode root) {
  }

  public void updateDatasetCaseSensitivity(JsonNode rootNode) {
  }

  public void updateDatasetReference(JsonNode rootNode) {
  }

  public void updateDatasetPartition(JsonNode rootNode) {
  }

  public void updateDatasetDeployment(JsonNode rootNode) {
  }

  public void updateDatasetConstraint(JsonNode rootNode) {
  }

  public void updateDatasetTags(JsonNode rootNode) {
  }

  public void updateDatasetIndex(JsonNode rootNode) {
  }

  public void updateDatasetCapacity(JsonNode rootNode) {
  }

  public void updateDatasetCompliance(JsonNode rootNode) {
  }

  public void updateDatasetSecurity(JsonNode rootNode) {
  }
}
