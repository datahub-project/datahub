package com.linkedin.metadata;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/** Utility class for calculating sizes of Avro records and DataHub RecordTemplates. */
public class AvroSizeCalculator {

  private AvroSizeCalculator() {
    // Utility class, prevent instantiation
  }

  /**
   * Calculate the serialized size of an Avro GenericRecord.
   *
   * @param record The Avro GenericRecord to measure
   * @return Size in bytes of the serialized record
   */
  public static long calculateAvroSize(@Nonnull GenericRecord record) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
      writer.write(record, encoder);
      encoder.flush();
      return out.size();
    } catch (IOException e) {
      throw new RuntimeException("Failed to calculate Avro record size", e);
    }
  }

  /**
   * Estimates the minimum size of a RecordTemplate by walking the DataMap. This is a LOWER BOUND -
   * the actual serialized JSON will be equal or larger due to escaping, structure overhead, and
   * encoding.
   *
   * <p>Uses character count (not UTF-8 bytes) for speed - assumes worst-case 3 bytes per character.
   *
   * @param aspect The RecordTemplate aspect to measure
   * @return Estimated minimum size in bytes (conservative - actual size will likely be larger)
   */
  public static long estimateAspectSize(@Nonnull RecordTemplate aspect) {
    long charCount = estimateDataMapSize(aspect.data());
    // Conservative: assume 3 bytes per character (worst-case UTF-8)
    return charCount * 3;
  }

  /**
   * Recursively estimates the character count of a DataMap structure. This counts field names,
   * string values, and structural characters ({}, [], :, ,) without actually serializing.
   *
   * @param dataMap The DataMap to measure
   * @return Estimated character count
   */
  private static long estimateDataMapSize(DataMap dataMap) {
    long size = 2; // Opening and closing braces: {}

    boolean first = true;
    for (java.util.Map.Entry<String, Object> entry : dataMap.entrySet()) {
      if (!first) {
        size += 1; // Comma between fields
      }
      first = false;

      // Field name: "key":
      size += entry.getKey().length() + 3; // quotes + colon

      // Field value
      size += estimateValueSize(entry.getValue());
    }

    return size;
  }

  /**
   * Estimates the size of a value (String, Number, Boolean, DataMap, DataList).
   *
   * @param value The value to measure
   * @return Estimated character count
   */
  private static long estimateValueSize(Object value) {
    if (value == null) {
      return 4; // "null"
    } else if (value instanceof String) {
      // String value with quotes
      return ((String) value).length() + 2;
    } else if (value instanceof Number) {
      // Worst case: Long.MAX_VALUE = 20 characters
      return 20;
    } else if (value instanceof Boolean) {
      return 5; // "false" is longest
    } else if (value instanceof DataMap) {
      return estimateDataMapSize((DataMap) value);
    } else if (value instanceof DataList) {
      return estimateDataListSize((DataList) value);
    } else {
      // Unknown type - conservative estimate
      return value.toString().length();
    }
  }

  /**
   * Recursively estimates the character count of a DataList structure.
   *
   * @param dataList The DataList to measure
   * @return Estimated character count
   */
  private static long estimateDataListSize(DataList dataList) {
    long size = 2; // Opening and closing brackets: []

    boolean first = true;
    for (Object item : dataList) {
      if (!first) {
        size += 1; // Comma between items
      }
      first = false;

      size += estimateValueSize(item);
    }

    return size;
  }
}
