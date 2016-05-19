package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Test;
import play.test.WithApplication;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;
import static play.test.Helpers.fakeApplication;
import static play.test.Helpers.running;

/**
 * Testing loading tree for UI.
 *
 * @author R.Kluszczynski
 */
public class TreeTest extends WithApplication {

    @Test
    public void shouldResponseWithEmptyJsonForNotExistingDatasetsFile() throws IOException {
        final Map<String, String> additionalConfiguration = Maps.newHashMap();
        additionalConfiguration.put("datasets.tree.source", "file");
        additionalConfiguration.put("datasets.tree.name", "not-existing-datasets-json-file");

        running(fakeApplication(additionalConfiguration), () -> {
            final JsonNode trie = Tree.loadTree("datasets");

            assertThat(trie.toString()).isEqualTo("{}");
        });
    }

    @Test
    public void shouldResponseWithEmptyJsonForUnsupportedSourceType() throws IOException {
        final Map<String, String> additionalConfiguration = Maps.newHashMap();
        additionalConfiguration.put("datasets.tree.source", "not-supported");

        running(fakeApplication(additionalConfiguration), () -> {
            final JsonNode trie = Tree.loadTree("datasets");

            assertThat(trie.toString()).isEqualTo("{}");
        });
    }

    @Test
    public void shouldReadDatasetsJsonResponseFromFile() throws IOException {
        final Map<String, String> additionalConfiguration = Maps.newHashMap();
        additionalConfiguration.put("datasets.tree.name", "test/resources/exampleDatasets.json");

        Arrays.asList("file", "", null).stream()
                .forEach(treeSource -> {
                    additionalConfiguration.put("datasets.tree.source", treeSource);

                    running(fakeApplication(additionalConfiguration), () -> {
                        final JsonNode trie = Tree.loadTree("datasets");

                        assertThat(trie.fieldNames().next()).isEqualTo("children");
                    });
                });
    }

    @Test
    public void shouldReadDatasetsJsonFromDatabase() throws IOException, SQLException, InterruptedException {
        final String jdbcUrl = "jdbc:h2:mem:testing-datasets-tree-db";
        createDatabaseWithDatasetsTree(jdbcUrl + ";DB_CLOSE_DELAY=-1");

        final Map<String, String> additionalConfiguration = Maps.newHashMap();
        additionalConfiguration.put("datasets.tree.source", "database");
        additionalConfiguration.put("datasets.tree.name", null);

        additionalConfiguration.put("database.opensource.driver", "org.h2.Driver");
        additionalConfiguration.put("database.opensource.username", "sa");
        additionalConfiguration.put("database.opensource.password", "");
        additionalConfiguration.put("database.opensource.url", jdbcUrl);

        running(fakeApplication(additionalConfiguration), () -> {
            final JsonNode trie = Tree.loadTree("datasets");

            assertThat(trie.fieldNames().next()).isEqualTo("children");
        });
    }

    private void createDatabaseWithDatasetsTree(String jdbcUrl) throws SQLException {
        DataSource dataSource = JdbcConnectionPool.create(jdbcUrl, "sa", "");
        Connection connection = dataSource.getConnection();
        connection.createStatement().executeUpdate("CREATE TABLE cfg_ui_trees (" +
                " name VARCHAR(32)," +
                " value LONGTEXT NOT NULL," +
                " PRIMARY KEY (name))");
        connection.createStatement().executeUpdate("INSERT INTO cfg_ui_trees VALUES ('datasets', '{\"children\": []}')");
        connection.close();
    }
}
