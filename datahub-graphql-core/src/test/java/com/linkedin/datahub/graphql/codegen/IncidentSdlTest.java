package com.linkedin.datahub.graphql.codegen;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.generated.IncidentEntityTypes;
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
        new HashSet<>(IncidentSdl.typesDeclaringIncidentsField(sdl)), Set.of("Dataset", "Chart"));
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
  public void ignoresResultWrapperIncidentsField() {
    String sdl =
        """
        type Incident { title: String }
        type EntityIncidentsResult {
          start: Int
          incidents: [Incident!]!
        }
        extend type Dataset {
          incidents(start: Int): EntityIncidentsResult
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
  public void generatedArtifactMatchesSdlParse() throws IOException {
    String sdl = readIncidentSdl();
    assertEquals(
        new HashSet<>(IncidentEntityTypes.ENTITY_TYPES),
        new HashSet<>(IncidentSdl.typesDeclaringIncidentsField(sdl)));
    assertTrue(
        IncidentEntityTypes.ENTITY_TYPES.contains("Dataset"),
        IncidentEntityTypes.ENTITY_TYPES.toString());
    assertTrue(
        IncidentEntityTypes.ENTITY_TYPES.contains("SchemaFieldEntity"),
        IncidentEntityTypes.ENTITY_TYPES.toString());
    assertFalse(IncidentEntityTypes.ENTITY_TYPES.contains("Mutation"));
    assertFalse(IncidentEntityTypes.ENTITY_TYPES.contains("Incident"));
    assertFalse(IncidentEntityTypes.ENTITY_TYPES.contains("EntityIncidentsResult"));
  }

  private static String readIncidentSdl() throws IOException {
    try (InputStream in =
        IncidentSdlTest.class.getClassLoader().getResourceAsStream("incident.graphql")) {
      assertNotNull(in, "incident.graphql missing from test classpath");
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
