package com.linkedin.datahub.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class Search {

  private static final ObjectMapper _OM = new ObjectMapper();

  private Search() {
  }

  public static String readJsonQueryFile(String jsonFile) {
    try {
      String contents = new String(Files.readAllBytes(Paths.get(jsonFile)));
      JsonNode json = _OM.readTree(contents);
      return json.toString();
    } catch (Exception e) {
      log.error("ReadJsonQueryFile failed. Error: " + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

}