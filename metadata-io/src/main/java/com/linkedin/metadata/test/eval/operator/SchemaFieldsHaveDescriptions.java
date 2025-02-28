package com.linkedin.metadata.test.eval.operator;

import com.linkedin.metadata.test.definition.expression.ExpressionType;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.operator.Operands;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.eval.ResolvedOperand;
import com.linkedin.metadata.test.eval.ResolvedOperands;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import com.linkedin.metadata.test.query.schemafield.SchemaField;
import com.linkedin.metadata.test.query.schemafield.TestsSchemaFieldUtils;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a special, purpose-built operator that checks whether a query result containing a list of
 * schema fields ALL have a description.
 *
 * <p>This is currently ONLY supported for operands of type {@link
 * com.linkedin.metadata.test.query.schemafield.SchemaField}. If the input provided is not this
 * type, this operator will return false.
 *
 * <p>In the future, we can extend this class to support other types of resolved entities. If there
 * are no schema fields at all, this operator will return false.
 */
@Slf4j
public class SchemaFieldsHaveDescriptions extends BaseOperatorEvaluator {

  @Override
  public OperatorType getOperatorType() {
    return OperatorType.SCHEMA_FIELDS_HAVE_DESCRIPTIONS;
  }

  @Override
  public void validate(Operands operands) throws InvalidOperandException {
    if (operands.size() != 1
        || !(ExpressionType.QUERY.equals(operands.get(0).getExpression().expressionType()))) {
      throw new InvalidOperandException(
          "Invalid params for the exists operation: Requires 1 quuery operands");
    }

    // Strict validation of the input, requiring a schemaFields query result.
    final Query query = (Query) operands.get(0).getExpression();
    if (!(TestsSchemaFieldUtils.SCHEMA_FIELDS_PROPERTY.equals(query.getQuery().getQuery()))) {
      throw new InvalidOperandException(
          "Invalid params for the exists operation: Requires schemaFields query result as input");
    }
  }

  @Override
  public Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException {
    final ResolvedOperand operand =
        resolvedOperands.get(0); // Query response -> This will be list of string or string.
    log.debug(
        String.format(
            "Invoking 'Schema fields have description' operator with resolved operand %s",
            operand.getExpression().getValue()));
    return allFieldsHaveDescriptions(operand.getExpression().getValue());
  }

  private boolean allFieldsHaveDescriptions(Object value) {
    if (value == null) {
      return false;
    }
    if (value instanceof List) {
      // Lists must be longer than 0 to "exist".
      List<?> list = (List<?>) value;
      // At least one of the list's objects must exist.
      return list.size() > 0 && list.stream().allMatch(this::hasDescription);
    }
    return true;
  }

  private boolean hasDescription(Object value) {
    if (value instanceof String) {
      // First, decode the string into a schema field.
      SchemaField schemaField = TestsSchemaFieldUtils.deserializeSchemaField((String) value);
      // Simply check whether the description is not null and not empty.
      return (schemaField.getDescription() != null && !schemaField.getDescription().isEmpty())
          || (schemaField.getEditableDescription() != null
              && !schemaField.getEditableDescription().isEmpty());
    }
    // TODO: Support dynamic resolution of other types of entities, or URNs directly.
    log.warn(
        String.format(
            "Provided bad input type %s to Schema Fields Have Descriptions operator!",
            value.getClass()));
    return false;
  }
}
