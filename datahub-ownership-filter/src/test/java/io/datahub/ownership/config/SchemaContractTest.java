package io.datahub.ownership.config;

import io.datahub.ownership.instrumentation.FieldArgumentMutators;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pre-flight check: every field name in our registry exists as a top-level Query field
 * in DataHub's GraphQL schema. If a field gets renamed upstream, this test fails loudly.
 */
class SchemaContractTest {

    @Test
    void allRegisteredFieldsExistInSchema() throws IOException {
        Path schemaDir = Path.of("../datahub-graphql-core/src/main/resources");
        Set<String> definedQueryFields;
        try (Stream<Path> stream = Files.walk(schemaDir)) {
            definedQueryFields = stream
                .filter(p -> p.toString().endsWith(".graphql"))
                .flatMap(SchemaContractTest::extractQueryFieldNames)
                .collect(java.util.stream.Collectors.toSet());
        }

        Set<String> registry = Set.of(
            "searchAcrossEntities", "scrollAcrossEntities", "searchAcrossLineage",
            "scrollAcrossLineage", "autoComplete", "autoCompleteForMultiple",
            "browse", "browseV2", "aggregateAcrossEntities", "search"
        );

        // Sanity: confirm registry matches FieldArgumentMutators
        FieldArgumentMutators m = new FieldArgumentMutators();
        for (String f : registry) {
            assertThat(m.isOwnershipGated(f)).as("registry consistency: %s", f).isTrue();
        }

        // The actual contract: each registered field must exist in the schema
        for (String f : registry) {
            assertThat(definedQueryFields)
                .as("Field '%s' from registry not found in any .graphql file under %s", f, schemaDir)
                .contains(f);
        }
    }

    private static Stream<String> extractQueryFieldNames(Path schemaFile) {
        try {
            String src = Files.readString(schemaFile, StandardCharsets.UTF_8);
            // Pull names from `extend type Query { ... }` blocks.
            Pattern queryBlock = Pattern.compile("extend type Query\\s*\\{([^}]*)\\}", Pattern.DOTALL);
            Pattern fieldDecl = Pattern.compile("(\\w+)\\s*\\([^)]*\\)\\s*:");
            Stream.Builder<String> names = Stream.builder();
            Matcher b = queryBlock.matcher(src);
            while (b.find()) {
                Matcher f = fieldDecl.matcher(b.group(1));
                while (f.find()) names.add(f.group(1));
            }
            return names.build();
        } catch (IOException e) {
            return Stream.empty();
        }
    }
}
