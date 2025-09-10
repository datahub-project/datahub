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
import java.util.List;
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

    SchemaField field1 = new SchemaField("path1", "description1", null, null);

    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    SchemaField field2 = new SchemaField("path2", null, "description2", null);

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

    SchemaField field1 = new SchemaField("path1", null, null, null);

    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    SchemaField field2 = new SchemaField("path2", null, null, null);

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

    SchemaField field1 = new SchemaField("path1", null, null, null);

    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    SchemaField field2 = new SchemaField("path2", null, "description2", null);

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

  @Test
  public void testEvaluateFieldsWithDocumentationAspect() {
    ResolvedOperand resolvedOperand = mock(ResolvedOperand.class);
    ResolvedExpression resolvedExpression = mock(ResolvedExpression.class);

    // Create a field with documentation aspect containing propagated documentation
    List<String> documentation = Arrays.asList("This is AI-generated documentation for the field");
    SchemaField field1 = new SchemaField("path1", null, null, documentation);
    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    // Create a field with both basic description and documentation aspect
    SchemaField field2 = new SchemaField("path2", "basic description", null, documentation);
    String serializedField2 = TestsSchemaFieldUtils.serializeSchemaField(field2);

    when(resolvedExpression.getValue())
        .thenReturn(Arrays.asList(serializedField1, serializedField2));
    when(resolvedOperand.getExpression()).thenReturn(resolvedExpression);

    when(resolvedOperands.get(0)).thenReturn(resolvedOperand);

    // Both fields should have descriptions (one from documentation aspect, one from basic
    // description)
    assertTrue((boolean) schemaFieldsHaveDescriptions.evaluate(resolvedOperands));
  }

  @Test
  public void testEvaluateFieldsWithEmptyDocumentationAspect() {
    ResolvedOperand resolvedOperand = mock(ResolvedOperand.class);
    ResolvedExpression resolvedExpression = mock(ResolvedExpression.class);

    // Create a field with empty documentation aspect
    List<String> documentation = Arrays.asList(""); // Empty documentation
    SchemaField field1 = new SchemaField("path1", null, null, documentation);
    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    when(resolvedExpression.getValue()).thenReturn(Arrays.asList(serializedField1));
    when(resolvedOperand.getExpression()).thenReturn(resolvedExpression);

    when(resolvedOperands.get(0)).thenReturn(resolvedOperand);

    // Field should not have valid description (empty documentation)
    assertFalse((boolean) schemaFieldsHaveDescriptions.evaluate(resolvedOperands));
  }

  @Test
  public void testEvaluateFieldsWithDocumentationAspectOnly() {
    ResolvedOperand resolvedOperand = mock(ResolvedOperand.class);
    ResolvedExpression resolvedExpression = mock(ResolvedExpression.class);

    // Create a field with only documentation aspect (no basic description)
    List<String> documentation = Arrays.asList("This is propagated documentation");
    SchemaField field1 = new SchemaField("path1", null, null, documentation);
    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    when(resolvedExpression.getValue()).thenReturn(Arrays.asList(serializedField1));
    when(resolvedOperand.getExpression()).thenReturn(resolvedExpression);

    when(resolvedOperands.get(0)).thenReturn(resolvedOperand);

    // Field should have valid description from documentation aspect
    assertTrue((boolean) schemaFieldsHaveDescriptions.evaluate(resolvedOperands));
  }

  @Test
  public void testEvaluateFieldsWithMixedDocumentationSources() {
    ResolvedOperand resolvedOperand = mock(ResolvedOperand.class);
    ResolvedExpression resolvedExpression = mock(ResolvedExpression.class);

    // Field with only basic description
    SchemaField field1 = new SchemaField("path1", "basic description", null, null);
    String serializedField1 = TestsSchemaFieldUtils.serializeSchemaField(field1);

    // Field with only editable description
    SchemaField field2 = new SchemaField("path2", null, "editable description", null);
    String serializedField2 = TestsSchemaFieldUtils.serializeSchemaField(field2);

    // Field with only documentation aspect
    List<String> documentation = Arrays.asList("documentation aspect description");
    SchemaField field3 = new SchemaField("path3", null, null, documentation);
    String serializedField3 = TestsSchemaFieldUtils.serializeSchemaField(field3);

    // Field with no descriptions
    SchemaField field4 = new SchemaField("path4", null, null, null);
    String serializedField4 = TestsSchemaFieldUtils.serializeSchemaField(field4);

    when(resolvedExpression.getValue())
        .thenReturn(
            Arrays.asList(serializedField1, serializedField2, serializedField3, serializedField4));
    when(resolvedOperand.getExpression()).thenReturn(resolvedExpression);

    when(resolvedOperands.get(0)).thenReturn(resolvedOperand);

    // Should return false because field4 has no descriptions
    assertFalse((boolean) schemaFieldsHaveDescriptions.evaluate(resolvedOperands));
  }
}
