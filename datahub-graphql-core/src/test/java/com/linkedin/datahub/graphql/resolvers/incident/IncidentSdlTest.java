package com.linkedin.datahub.graphql.resolvers.incident;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class IncidentSdlTest {

  @Test
  public void discoversTypesFromExtendBlocks() {
    String sdl =
        """
        type EntityIncidentsResult { total: Int }
        extend type Mutation { raiseIncident(input: String): String }
        extend type Dataset {
          incidents(start: Int): EntityIncidentsResult
        }
        extend type Chart {
          incidents(start: Int): EntityIncidentsResult
        }
        """;

    assertEquals(
        new HashSet<>(IncidentSdl.typesDeclaringIncidentsField(sdl)),
        Set.of("Dataset", "Chart"));
  }

  @Test
  public void discoversTypesFromObjectDefinitions() {
    String sdl =
        """
        type EntityIncidentsResult { total: Int }
        type Dataset {
          incidents: EntityIncidentsResult
        }
        type Incident {
          title: String
        }
        """;

    assertEquals(IncidentSdl.typesDeclaringIncidentsField(sdl), List.of("Dataset"));
  }

  @Test
  public void ignoresRootOperationTypesEvenIfTheyDeclareIncidents() {
    String sdl =
        """
        type EntityIncidentsResult { total: Int }
        extend type Mutation {
          incidents: EntityIncidentsResult
        }
        """;

    assertEquals(IncidentSdl.typesDeclaringIncidentsField(sdl), List.of());
  }

  @Test
  public void realIncidentSdlWiresEntityTypesNotIncidentDomainTypes() throws IOException {
    List<String> types = IncidentSdl.typesDeclaringIncidentsField(readIncidentSdl());

    assertTrue(types.contains("Dataset"), types.toString());
    assertTrue(types.contains("DataJob"), types.toString());
    assertTrue(types.contains("MLModel"), types.toString());
    assertTrue(types.contains("MLFeatureTable"), types.toString());
    assertTrue(types.contains("SchemaFieldEntity"), types.toString());
    assertFalse(types.contains("Mutation"), types.toString());
    assertFalse(types.contains("Incident"), types.toString());
    assertFalse(types.contains("EntityIncidentsResult"), types.toString());
  }

  private static String readIncidentSdl() throws IOException {
    try (InputStream in =
        IncidentSdlTest.class.getClassLoader().getResourceAsStream("incident.graphql")) {
      assertNotNull(in, "incident.graphql missing from test classpath");
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
