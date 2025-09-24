package com.linkedin.datahub.graphql.resolvers.settings.ai;

import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AiInstructionInput;
import com.linkedin.datahub.graphql.generated.AiInstructionType;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Utility class for validating AI instruction inputs. Performs validation to ensure data integrity
 * and business rules.
 */
public class AiInstructionValidationUtils {

  private AiInstructionValidationUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Validates a list of AI instruction inputs according to business rules.
   *
   * @param instructions the list of AI instruction inputs to validate
   * @throws DataHubGraphQLException if validation fails
   */
  public static void validateAiInstructions(@Nonnull List<AiInstructionInput> instructions) {
    if (instructions == null || instructions.isEmpty()) {
      return; // Nothing to validate
    }

    validateNoDuplicateIds(instructions);
    validateSingleGeneralContext(instructions);
  }

  /**
   * Validates that there are no duplicate instruction IDs.
   *
   * @param instructions the list of AI instruction inputs to validate
   * @throws DataHubGraphQLException if duplicate IDs are found
   */
  private static void validateNoDuplicateIds(@Nonnull List<AiInstructionInput> instructions) {
    Set<String> seenIds = new HashSet<>();
    Set<String> duplicateIds = new HashSet<>();

    for (AiInstructionInput instruction : instructions) {
      String id = instruction.getId();
      if (id != null && !id.trim().isEmpty()) {
        String trimmedId = id.trim();
        if (seenIds.contains(trimmedId)) {
          duplicateIds.add(trimmedId);
        } else {
          seenIds.add(trimmedId);
        }
      }
    }

    if (!duplicateIds.isEmpty()) {
      String duplicateIdsList = String.join(", ", duplicateIds);
      throw new DataHubGraphQLException(
          String.format(
              "Duplicate instruction IDs found: %s. Each instruction must have a unique ID.",
              duplicateIdsList),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }

  /**
   * Validates that there is at most one instruction of type GENERAL_CONTEXT.
   *
   * @param instructions the list of AI instruction inputs to validate
   * @throws DataHubGraphQLException if multiple GENERAL_CONTEXT instructions are found
   */
  private static void validateSingleGeneralContext(@Nonnull List<AiInstructionInput> instructions) {
    long generalContextCount =
        instructions.stream()
            .filter(instruction -> instruction.getType() == AiInstructionType.GENERAL_CONTEXT)
            .count();

    if (generalContextCount > 1) {
      throw new DataHubGraphQLException(
          String.format(
              "Found %d instructions of type GENERAL_CONTEXT. Only one GENERAL_CONTEXT instruction is allowed.",
              generalContextCount),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }
}
