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

package datamigration;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import metadata.etl.EtlJob;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import wherehows.common.Constant;
import wherehows.common.utils.JdbcConnection;
import wherehows.restli.dataset.client.EspressoCreateSchemaMetadata;


public class EspressoPopulateSchemaMetadataEtl extends EtlJob {

  @Deprecated
  public EspressoPopulateSchemaMetadataEtl(int dbId, long whExecId) {
    super(null, dbId, whExecId);
  }

  public EspressoPopulateSchemaMetadataEtl(int dbId, long whExecId, Properties prop) {
    super(null, dbId, whExecId, prop);
  }

  public static final String GET_OWNER_BY_ID = "SELECT * FROM dataset_owner WHERE dataset_urn like :platform limit 1";
  public final static String GET_DATASETS_NAME_LIKE_PLAT =
      "SELECT * FROM dict_dataset WHERE urn like :platform";


  /** The property_name field in wh_property table for WhereHows database connection information */
  public String driverClassName = prop.getProperty(Constant.WH_DB_DRIVER_KEY);
  public String url = prop.getProperty(Constant.WH_DB_URL_KEY);
  public String dbUserName = prop.getProperty(Constant.WH_DB_USERNAME_KEY);
  public String dbPassword = prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
  public String restLiServerURL = prop.getProperty(Constant.WH_RESTLI_SERVER_URL);

  EspressoCreateSchemaMetadata lRestClient = new EspressoCreateSchemaMetadata();

  public void populate()
      throws SQLException {

    // Create an HttpClient and wrap it in an abstraction layer
    final HttpClientFactory http = new HttpClientFactory();
    final Client r2Client = new TransportClientAdapter(http.getClient(Collections.<String, String>emptyMap()));
    RestClient _restClient = new RestClient(r2Client, restLiServerURL);
    logger.debug("restLiServerURL is: " + restLiServerURL);

    NamedParameterJdbcTemplate
        lNamedParameterJdbcTemplate = JdbcConnection.getNamedParameterJdbcTemplate(driverClassName,url,dbUserName,dbPassword);

    Map<String, Object> OwnerParams = new HashMap<>();
    Map<String, Object> ownerQueryResult = new HashMap<>();
    OwnerParams.put("platform", "espresso:///%");
    ownerQueryResult = lNamedParameterJdbcTemplate.queryForMap(GET_OWNER_BY_ID, OwnerParams);

    Map<String, Object> params = new HashMap<>();
    params.put("platform", "espresso:///%");
    List<Map<String, Object>> rows = null;
    rows = lNamedParameterJdbcTemplate.queryForList(GET_DATASETS_NAME_LIKE_PLAT, params);
    logger.info("Total number of rows is: " + rows.size());

    for (Map row : rows) {

      String urn = (String) row.get("urn");
      String lDatasetName = urn.substring(12).replace('/','.');
      String lSchemaName = lDatasetName;
      String lOwnerName = ownerQueryResult.get("namespace") + ":" + ownerQueryResult.get("owner_id");

      String lSchemaJsonString = (String)row.get("schema");

      logger.info("The populating schema is: " + lSchemaJsonString);
      String lTableSchemaData = null;
      String lDocumentSchemaData = null;

      try {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(lSchemaJsonString);

        ArrayNode keySchemaNode = (ArrayNode) rootNode.get("keySchema");
        ObjectNode tableSchemaNode = mapper.createObjectNode();
        ArrayNode resourceKeyPartsNode = mapper.createArrayNode();

        if (keySchemaNode!= null)
        {
          Iterator<JsonNode> slaidsIterator = keySchemaNode.getElements();
          while (slaidsIterator.hasNext())
          {
            JsonNode oneElementNode = slaidsIterator.next();
            resourceKeyPartsNode.add(oneElementNode);
          }

          String name = "";
          String doc = "";
          if (rootNode.has("name") && rootNode.has("doc"))
          {
            name = rootNode.get("name").toString();
            doc = rootNode.get("doc").toString();

            tableSchemaNode.put("name",name);
            tableSchemaNode.put("doc",doc);
            tableSchemaNode.put("schemaType","TableSchema");
            tableSchemaNode.put("recordType","/schemata/document/DUMMY/DUMMY");
            tableSchemaNode.put("version",1);
            tableSchemaNode.put("resourceKeyParts",resourceKeyPartsNode);

            lTableSchemaData = tableSchemaNode.toString();
          }
          else
          {
            logger.info("Missing name or doc field. Corrupted schema : " + lSchemaName);
          }
         }


          JsonNode valueSchemaNode = rootNode.get("valueSchema");
          if (valueSchemaNode != null && valueSchemaNode.has("name"))
          {
            lDocumentSchemaData = valueSchemaNode.toString();
          }
          else
          {
            logger.info("ValueSchema is missing name field in schema : " + lSchemaName);
          }
      }
      catch (JsonParseException e) { e.printStackTrace(); }
      catch (JsonMappingException e) { e.printStackTrace(); }
      catch (IOException e) { e.printStackTrace(); }

      if (lTableSchemaData != null && lDocumentSchemaData != null)
      {
        try {
          logger.info("table schema: " + lTableSchemaData);
          logger.info("valueSchema: " + lDocumentSchemaData);
          System.out.println("lSchemaName is " + lSchemaName);

          lRestClient.create(_restClient, lSchemaName,lTableSchemaData,lDocumentSchemaData,lDatasetName,lOwnerName);
        } catch (Exception e) {
          logger.error(e.toString());
        }
      }
    }

    // shutdown
    _restClient.shutdown(new FutureCallback<None>());
    http.shutdown(new FutureCallback<None>());

  }

  @Override
  public void extract()
      throws Exception {
  }

  @Override
  public void transform()
      throws Exception {
  }

  @Override
  public void load()
      throws Exception {
  }

}
