package com.linkedin.metadata.test.eval.operator;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.test.definition.expression.Expression;
import com.linkedin.metadata.test.definition.expression.ExpressionType;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.Operands;
import com.linkedin.metadata.test.eval.ResolvedExpression;
import com.linkedin.metadata.test.eval.ResolvedOperand;
import com.linkedin.metadata.test.eval.ResolvedOperands;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.schemafield.SchemaField;
import com.linkedin.metadata.test.query.schemafield.TestsSchemaFieldUtils;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaFieldsHaveDescriptionsTest {

  private SchemaFieldsHaveDescriptions schemaFieldsHaveDescriptions;
  private Operands operands;
  private ResolvedOperands resolvedOperands;

  @BeforeMethod
  public void setUp() {
    schemaFieldsHaveDescriptions = new SchemaFieldsHaveDescriptions();
    operands = mock(Operands.class);
    resolvedOperands = mock(ResolvedOperands.class);
  }

  @Test(expectedExceptions = InvalidOperandException.class)
  public void testValidateInvalidNumberOfOperands() throws InvalidOperandException {
    when(operands.size()).thenReturn(0);
    schemaFieldsHaveDescriptions.validate(operands);
  }

  @Test(expectedExceptions = InvalidOperandException.class)
  public void testValidateInvalidOperandType() throws InvalidOperandException {
    when(operands.size()).thenReturn(1);
    Expression expression = mock(Expression.class);
    when(expression.expressionType()).thenReturn(ExpressionType.LITERAL); // Incorrect type

    Operand operand = mock(Operand.class);
    when(operand.getExpression()).thenReturn(expression);
    when(operands.get(0)).thenReturn(operand);
    schemaFieldsHaveDescriptions.validate(operands);
  }

  @Test
  public void testValidateValidInput() throws InvalidOperandException {
    when(operands.size()).thenReturn(1);

    Query query = mock(Query.class);
    when(query.getQuery()).thenReturn(new TestQuery("schemaFields"));
    when(query.expressionType()).thenReturn(ExpressionType.QUERY);

    Operand operand = mock(Operand.class);
    when(operand.getExpression()).thenReturn(query);
    when(operands.get(0)).thenReturn(operand);
    schemaFieldsHaveDescriptions.validate(operands);
  }

  @Test
  public void testEvaluateAllFieldsHaveDescriptions() {
    ResolvedOperand resolvedOperand = mock(ResolvedOperand.class);
    ResolvedExpression resolvedExpression = mock(ResolvedExpression.class);

    SchemaField field1 = new SchemaField("path1", "description1", null);

    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    SchemaField field2 = new SchemaField("path2", null, "description2");

    String serializedField2 = TestsSchemaFieldUtils.serializeSchemaField(field2);

    when(resolvedExpression.getValue())
        .thenReturn(Arrays.asList(serializedField1, serializedField2));
    when(resolvedOperand.getExpression()).thenReturn(resolvedExpression);

    when(resolvedOperands.get(0)).thenReturn(resolvedOperand);

    assertTrue((boolean) schemaFieldsHaveDescriptions.evaluate(resolvedOperands));
  }

  @Test
  public void testEvaluateNoFieldsHaveDescriptions() {
    ResolvedOperand resolvedOperand = mock(ResolvedOperand.class);
    ResolvedExpression resolvedExpression = mock(ResolvedExpression.class);

    SchemaField field1 = new SchemaField("path1", null, null);

    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    SchemaField field2 = new SchemaField("path2", null, null);

    String serializedField2 = TestsSchemaFieldUtils.serializeSchemaField(field2);

    when(resolvedExpression.getValue())
        .thenReturn(Arrays.asList(serializedField1, serializedField2));
    when(resolvedOperand.getExpression()).thenReturn(resolvedExpression);

    when(resolvedOperands.get(0)).thenReturn(resolvedOperand);

    assertFalse((boolean) schemaFieldsHaveDescriptions.evaluate(resolvedOperands));
  }

  @Test
  public void testEvaluateSomeFieldsHaveDescriptions() {
    ResolvedOperand resolvedOperand = mock(ResolvedOperand.class);
    ResolvedExpression resolvedExpression = mock(ResolvedExpression.class);

    SchemaField field1 = new SchemaField("path1", null, null);

    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    SchemaField field2 = new SchemaField("path2", null, "description2");

    String serializedField2 = TestsSchemaFieldUtils.serializeSchemaField(field2);

    when(resolvedExpression.getValue())
        .thenReturn(Arrays.asList(serializedField1, serializedField2));
    when(resolvedOperand.getExpression()).thenReturn(resolvedExpression);

    when(resolvedOperands.get(0)).thenReturn(resolvedOperand);

    assertFalse((boolean) schemaFieldsHaveDescriptions.evaluate(resolvedOperands));
  }

  @Test
  public void testEvaluateEmptyFields() {
    ResolvedOperand resolvedOperand = mock(ResolvedOperand.class);
    ResolvedExpression resolvedExpression = mock(ResolvedExpression.class);

    when(resolvedExpression.getValue()).thenReturn(Collections.emptyList());
    when(resolvedOperand.getExpression()).thenReturn(resolvedExpression);

    when(resolvedOperands.get(0)).thenReturn(resolvedOperand);

    // No fields at all!
    assertFalse((boolean) schemaFieldsHaveDescriptions.evaluate(resolvedOperands));
  }
}
