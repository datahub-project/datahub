package com.linkedin.datahub.graphql.codegen;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;

public class GenerateListIncidentsQueryTest {

  @Test
  public void ordersLegacyTypesFirstThenRemainingAlphabetically() {
    assertEquals(
        GenerateListIncidentsQuery.orderTypes(List.of("MLModel", "Chart", "Dataset", "MLFeature")),
        List.of("Dataset", "Chart", "MLFeature", "MLModel"));
  }

  @Test
  public void legacyGmsTypeSetIsFrozen() {
    assertEquals(
        GenerateListIncidentsQuery.LEGACY_GMS_INCIDENT_TYPES,
        List.of("Dataset", "DataJob", "DataFlow", "Dashboard", "Chart"));
  }

  @Test
  public void leavesLegacyTypesUntaggedAndTagsNewerTypes() {
    String gql = GenerateListIncidentsQuery.render(List.of("MLModel", "Dataset"));

    assertTrue(gql.contains("query listEntityIncidents"));
    assertTrue(gql.contains("fragment entityIncidentsResultFields on EntityIncidentsResult"));
    assertTrue(gql.contains("title"));
    assertTrue(gql.contains("incidentStatus"));

    String datasetLine = lineContaining(gql, "... on Dataset {");
    String modelLine = lineContaining(gql, "... on MLModel {");
    assertFalse(datasetLine.contains("#[NEWER_GMS]"), datasetLine);
    assertTrue(modelLine.contains("#[NEWER_GMS]"), modelLine);
    assertTrue(gql.contains("... on MLModel { #[NEWER_GMS]"));
    assertFalse(gql.contains("... on Dataset { #[NEWER_GMS]"));
  }

  @Test
  public void realSdlKeepsFrozenLegacySetUntagged() throws IOException {
    String sdl = readIncidentSdl();
    String gql = GenerateListIncidentsQuery.render(IncidentSdl.typesDeclaringIncidentsField(sdl));

    for (String legacy : GenerateListIncidentsQuery.LEGACY_GMS_INCIDENT_TYPES) {
      String line = lineContaining(gql, "... on " + legacy + " {");
      assertFalse(line.contains("#[NEWER_GMS]"), line);
    }
    assertTrue(lineContaining(gql, "... on SchemaFieldEntity {").contains("#[NEWER_GMS]"));
    assertTrue(lineContaining(gql, "... on MLFeatureTable {").contains("#[NEWER_GMS]"));
    assertTrue(lineContaining(gql, "... on MLModel {").contains("#[NEWER_GMS]"));
  }

  private static String lineContaining(String gql, String needle) {
    return gql.lines()
        .filter(line -> line.contains(needle))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing " + needle + " in:\n" + gql));
  }

  private static String readIncidentSdl() throws IOException {
    try (InputStream in =
        GenerateListIncidentsQueryTest.class
            .getClassLoader()
            .getResourceAsStream("incident.graphql")) {
      assertNotNull(in, "incident.graphql missing from test classpath");
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
