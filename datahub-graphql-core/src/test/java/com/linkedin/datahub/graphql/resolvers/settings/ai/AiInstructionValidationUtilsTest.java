package com.linkedin.datahub.graphql.resolvers.settings.ai;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AiInstructionInput;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.AiInstructionType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class AiInstructionValidationUtilsTest {

  @Test
  public void testValidateAiInstructionsNullList() {
    // Should not throw exception for null list
    AiInstructionValidationUtils.validateAiInstructions(null);
  }

  @Test
  public void testValidateAiInstructionsEmptyList() {
    // Should not throw exception for empty list
    AiInstructionValidationUtils.validateAiInstructions(new ArrayList<>());
  }

  @Test
  public void testValidateAiInstructionsValidInstructions() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput("id1", AiInstructionType.GENERAL_CONTEXT, "instruction1"));

    // Should not throw exception for valid instructions
    AiInstructionValidationUtils.validateAiInstructions(instructions);
  }

  @Test
  public void testValidateAiInstructionsValidInstructionsWithNullIds() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput(null, AiInstructionType.GENERAL_CONTEXT, "instruction1"));

    // Should not throw exception when IDs are null, empty, or whitespace (they will be generated)
    AiInstructionValidationUtils.validateAiInstructions(instructions);
  }

  @Test
  public void testValidateAiInstructionsDuplicateIds() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput(
                "duplicate-id", AiInstructionType.GENERAL_CONTEXT, "instruction1"),
            createAiInstructionInput(
                "duplicate-id", AiInstructionType.GENERAL_CONTEXT, "instruction2"));

    DataHubGraphQLException exception =
        expectThrows(
            DataHubGraphQLException.class,
            () -> AiInstructionValidationUtils.validateAiInstructions(instructions));

    assertEquals(exception.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    assertTrue(exception.getMessage().contains("Duplicate instruction IDs found: duplicate-id"));
    assertTrue(exception.getMessage().contains("Each instruction must have a unique ID"));
  }

  @Test
  public void testValidateAiInstructionsMultipleDuplicateIds() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput("id1", AiInstructionType.GENERAL_CONTEXT, "instruction1"),
            createAiInstructionInput("id1", AiInstructionType.GENERAL_CONTEXT, "instruction2"),
            createAiInstructionInput("id2", AiInstructionType.GENERAL_CONTEXT, "instruction3"),
            createAiInstructionInput("id2", AiInstructionType.GENERAL_CONTEXT, "instruction4"));

    DataHubGraphQLException exception =
        expectThrows(
            DataHubGraphQLException.class,
            () -> AiInstructionValidationUtils.validateAiInstructions(instructions));

    assertEquals(exception.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    assertTrue(exception.getMessage().contains("Duplicate instruction IDs found:"));
    assertTrue(exception.getMessage().contains("id1"));
    assertTrue(exception.getMessage().contains("id2"));
  }

  @Test
  public void testValidateAiInstructionsDuplicateIdsWithWhitespace() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput("  id1  ", AiInstructionType.GENERAL_CONTEXT, "instruction1"),
            createAiInstructionInput("id1", AiInstructionType.GENERAL_CONTEXT, "instruction2"));

    DataHubGraphQLException exception =
        expectThrows(
            DataHubGraphQLException.class,
            () -> AiInstructionValidationUtils.validateAiInstructions(instructions));

    assertEquals(exception.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    assertTrue(exception.getMessage().contains("Duplicate instruction IDs found: id1"));
  }

  @Test
  public void testValidateAiInstructionsSingleGeneralContext() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput("id1", AiInstructionType.GENERAL_CONTEXT, "instruction1"));

    // Should not throw exception for single GENERAL_CONTEXT instruction
    AiInstructionValidationUtils.validateAiInstructions(instructions);
  }

  @Test
  public void testValidateAiInstructionsMultipleGeneralContext() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput("id1", AiInstructionType.GENERAL_CONTEXT, "instruction1"),
            createAiInstructionInput("id2", AiInstructionType.GENERAL_CONTEXT, "instruction2"));

    DataHubGraphQLException exception =
        expectThrows(
            DataHubGraphQLException.class,
            () -> AiInstructionValidationUtils.validateAiInstructions(instructions));

    assertEquals(exception.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    assertTrue(exception.getMessage().contains("Found 2 instructions of type GENERAL_CONTEXT"));
    assertTrue(exception.getMessage().contains("Only one GENERAL_CONTEXT instruction is allowed"));
  }

  @Test
  public void testValidateAiInstructionsThreeGeneralContext() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput("id1", AiInstructionType.GENERAL_CONTEXT, "instruction1"),
            createAiInstructionInput("id2", AiInstructionType.GENERAL_CONTEXT, "instruction2"),
            createAiInstructionInput("id3", AiInstructionType.GENERAL_CONTEXT, "instruction3"));

    DataHubGraphQLException exception =
        expectThrows(
            DataHubGraphQLException.class,
            () -> AiInstructionValidationUtils.validateAiInstructions(instructions));

    assertEquals(exception.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    assertTrue(exception.getMessage().contains("Found 3 instructions of type GENERAL_CONTEXT"));
  }

  @Test
  public void testValidateAiInstructionsBothDuplicateIdsAndMultipleGeneralContext() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput(
                "duplicate-id", AiInstructionType.GENERAL_CONTEXT, "instruction1"),
            createAiInstructionInput(
                "duplicate-id", AiInstructionType.GENERAL_CONTEXT, "instruction2"),
            createAiInstructionInput("id3", AiInstructionType.GENERAL_CONTEXT, "instruction3"));

    // Should fail on the first validation (duplicate IDs) before checking for multiple
    // GENERAL_CONTEXT
    DataHubGraphQLException exception =
        expectThrows(
            DataHubGraphQLException.class,
            () -> AiInstructionValidationUtils.validateAiInstructions(instructions));

    assertEquals(exception.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    assertTrue(exception.getMessage().contains("Duplicate instruction IDs found: duplicate-id"));
  }

  @Test
  public void testValidateAiInstructionsMixedValidAndInvalidIds() {
    List<AiInstructionInput> instructions =
        Arrays.asList(
            createAiInstructionInput("valid-id", AiInstructionType.GENERAL_CONTEXT, "instruction1"),
            createAiInstructionInput(
                null, AiInstructionType.GENERAL_CONTEXT, "instruction2"), // null ID is fine
            createAiInstructionInput(
                "duplicate", AiInstructionType.GENERAL_CONTEXT, "instruction3"),
            createAiInstructionInput(
                "duplicate", AiInstructionType.GENERAL_CONTEXT, "instruction4"));

    DataHubGraphQLException exception =
        expectThrows(
            DataHubGraphQLException.class,
            () -> AiInstructionValidationUtils.validateAiInstructions(instructions));

    assertEquals(exception.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    assertTrue(exception.getMessage().contains("Duplicate instruction IDs found: duplicate"));
  }

  /** Helper method to create an AiInstructionInput for testing. */
  private AiInstructionInput createAiInstructionInput(
      String id, AiInstructionType type, String instruction) {
    AiInstructionInput input = new AiInstructionInput();
    input.setId(id);
    input.setType(type);
    input.setState(AiInstructionState.ACTIVE);
    input.setInstruction(instruction);
    return input;
  }
}
