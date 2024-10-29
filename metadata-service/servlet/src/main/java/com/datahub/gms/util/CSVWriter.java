package com.datahub.gms.util;

import java.io.PrintWriter;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.WeightBuilder;

@Builder
public class CSVWriter {
  private PrintWriter printWriter;

  public CSVWriter println(String[] data) {
    printWriter.println(convertToCSV(data));
    return this;
  }

  private static String convertToCSV(String[] data) {
    return Stream.of(data).map(CSVWriter::escapeSpecialCharacters).collect(Collectors.joining(","));
  }

  private static String escapeSpecialCharacters(String data) {
    String escapedData = data.replaceAll("\\R", " ");
    if (data.contains(",") || data.contains("\"") || data.contains("'")) {
      data = data.replace("\"", "\"\"");
      escapedData = "\"" + data + "\"";
    }
    return escapedData;
  }

  public static String builderToString(FieldValueFactorFunctionBuilder in) {
    return String.format(
        "{\"field\":\"%s\",\"factor\":%s,\"missing\":%s,\"modifier\":\"%s\"}",
        in.fieldName(), in.factor(), in.missing(), in.modifier());
  }

  public static String builderToString(WeightBuilder in) {
    return String.format("{\"weight\":%s}", in.getWeight());
  }
}
