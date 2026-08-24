package com.linkedin.datahub.graphql.codegen;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reads incident.graphql and writes {@code IncidentEntityTypes} so GMS does not parse the SDL at
 * runtime to decide which types get {@code EntityIncidentsResolver}.
 */
public final class GenerateIncidentEntityTypes {

  private GenerateIncidentEntityTypes() {}

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "Usage: GenerateIncidentEntityTypes <incident.graphql> <IncidentEntityTypes.java>");
    }
    Path sdlPath = Path.of(args[0]);
    Path outPath = Path.of(args[1]);
    List<String> types = IncidentSdl.typesDeclaringIncidentsField(Files.readString(sdlPath));
    if (types.isEmpty()) {
      throw new IllegalStateException(
          sdlPath
              + " declares no incidents: EntityIncidentsResult fields; refusing to generate an empty list");
    }
    List<String> sorted = new ArrayList<>(types);
    Collections.sort(sorted);
    Files.createDirectories(outPath.getParent());
    Files.writeString(outPath, render(sorted), StandardCharsets.UTF_8);
  }

  static String render(List<String> types) {
    String names =
        types.stream()
            .map(type -> "\"" + type + "\"")
            .collect(Collectors.joining(",\n                  "));
    return """
        package com.linkedin.datahub.graphql.generated;

        import java.util.List;

        /** GraphQL types with {@code incidents: EntityIncidentsResult}. Generated — do not edit. */
        public final class IncidentEntityTypes {
          private IncidentEntityTypes() {}

          public static final List<String> ENTITY_TYPES =
              List.of(
                  %s);
        }
        """
        .formatted(names);
  }
}
