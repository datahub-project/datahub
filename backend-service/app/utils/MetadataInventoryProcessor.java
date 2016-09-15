package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import models.daos.DatasetInfoDao;
import org.apache.avro.generic.GenericData;
import wherehows.common.schemas.Record;


public class MetadataInventoryProcessor {

  /**
   * Process a MetadataInventoryEvent record
   * @param record GenericData.Record
   * @param topic String
   * @throws Exception
   * @return null
   */
  public Record process(GenericData.Record record, String topic)
      throws Exception {
    if (record != null) {
      // Logger.info("Processing Metadata Inventory Event record. ");

      final GenericData.Record auditHeader = (GenericData.Record) record.get("auditHeader");

      final JsonNode rootNode = new ObjectMapper().readTree(record.toString());
      DatasetInfoDao.updateDatasetInventory(rootNode);
    }
    return null;
  }
}
