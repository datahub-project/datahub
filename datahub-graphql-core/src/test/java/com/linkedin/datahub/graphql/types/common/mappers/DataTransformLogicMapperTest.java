package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.DataTransform;
import com.linkedin.common.DataTransformArray;
import com.linkedin.common.DataTransformLogic;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryStatement;
import java.util.Arrays;
import org.testng.annotations.Test;

public class DataTransformLogicMapperTest {

  @Test
  public void testMapWithQueryStatement() throws Exception {
    // Create test data
    DataTransformLogic input = new DataTransformLogic();

    // Create a transform with query statement
    DataTransform transform1 = new DataTransform();
    QueryStatement statement = new QueryStatement();
    statement.setValue("SELECT * FROM source_table");
    statement.setLanguage(QueryLanguage.SQL);
    transform1.setQueryStatement(statement);

    // Create another transform
    DataTransform transform2 = new DataTransform();
    QueryStatement statement2 = new QueryStatement();
    statement2.setValue("INSERT INTO target_table SELECT * FROM temp_table");
    statement2.setLanguage(QueryLanguage.SQL);
    transform2.setQueryStatement(statement2);

    // Set transforms
    input.setTransforms(new DataTransformArray(Arrays.asList(transform1, transform2)));

    // Map the object
    com.linkedin.datahub.graphql.generated.DataTransformLogic result =
        DataTransformLogicMapper.map(null, input);

    // Verify result
    assertNotNull(result);
    assertEquals(result.getTransforms().size(), 2);

    // Verify first transform
    com.linkedin.datahub.graphql.generated.DataTransform resultTransform1 =
        result.getTransforms().get(0);
    assertNotNull(resultTransform1.getQueryStatement());
    assertEquals(resultTransform1.getQueryStatement().getValue(), "SELECT * FROM source_table");
    assertEquals(resultTransform1.getQueryStatement().getLanguage().toString(), "SQL");

    // Verify second transform
    com.linkedin.datahub.graphql.generated.DataTransform resultTransform2 =
        result.getTransforms().get(1);
    assertNotNull(resultTransform2.getQueryStatement());
    assertEquals(
        resultTransform2.getQueryStatement().getValue(),
        "INSERT INTO target_table SELECT * FROM temp_table");
    assertEquals(resultTransform2.getQueryStatement().getLanguage().toString(), "SQL");
  }

  @Test
  public void testMapWithoutQueryStatement() throws Exception {
    // Create test data
    DataTransformLogic input = new DataTransformLogic();

    // Create a transform without query statement
    DataTransform transform = new DataTransform();

    // Set transforms
    input.setTransforms(new DataTransformArray(Arrays.asList(transform)));

    // Map the object
    com.linkedin.datahub.graphql.generated.DataTransformLogic result =
        DataTransformLogicMapper.map(null, input);

    // Verify result
    assertNotNull(result);
    assertEquals(result.getTransforms().size(), 1);

    // Verify transform
    com.linkedin.datahub.graphql.generated.DataTransform resultTransform =
        result.getTransforms().get(0);
    assertNull(resultTransform.getQueryStatement());
  }

  @Test
  public void testMapWithEmptyTransforms() throws Exception {
    // Create test data
    DataTransformLogic input = new DataTransformLogic();
    input.setTransforms(new DataTransformArray(Arrays.asList()));

    // Map the object
    com.linkedin.datahub.graphql.generated.DataTransformLogic result =
        DataTransformLogicMapper.map(null, input);

    // Verify result
    assertNotNull(result);
    assertEquals(result.getTransforms().size(), 0);
  }
}
