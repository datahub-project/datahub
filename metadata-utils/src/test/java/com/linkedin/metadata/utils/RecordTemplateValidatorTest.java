package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.schema.validator.Validator;
import com.linkedin.data.template.RecordTemplate;
import java.util.function.Consumer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RecordTemplateValidatorTest {

  @Mock private RecordTemplate mockRecord;

  @Mock private ValidationResult mockValidationResult;

  @Mock private Consumer<ValidationResult> mockValidationFailureHandler;

  @Mock private Validator mockValidator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testValidate_WhenValidationSucceeds_DoesNotCallFailureHandler() {
    // Arrange
    try (var mockedStatic = mockStatic(ValidateDataAgainstSchema.class)) {
      when(mockValidationResult.isValid()).thenReturn(true);
      mockedStatic
          .when(
              () ->
                  ValidateDataAgainstSchema.validate(
                      any(RecordTemplate.class),
                      any(ValidationOptions.class),
                      any(Validator.class)))
          .thenReturn(mockValidationResult);

      // Act
      RecordTemplateValidator.validate(mockRecord, mockValidationFailureHandler);

      // Assert
      verify(mockValidationFailureHandler, never()).accept(any(ValidationResult.class));
    }
  }

  @Test
  public void testValidate_WhenValidationFails_CallsFailureHandler() {
    // Arrange
    try (var mockedStatic = mockStatic(ValidateDataAgainstSchema.class)) {
      when(mockValidationResult.isValid()).thenReturn(false);
      mockedStatic
          .when(
              () ->
                  ValidateDataAgainstSchema.validate(
                      any(RecordTemplate.class),
                      any(ValidationOptions.class),
                      any(Validator.class)))
          .thenReturn(mockValidationResult);

      // Act
      RecordTemplateValidator.validate(mockRecord, mockValidationFailureHandler);

      // Assert
      verify(mockValidationFailureHandler).accept(mockValidationResult);
    }
  }

  @Test
  public void testValidateTrim_WhenValidationSucceeds_DoesNotCallFailureHandler() {
    // Arrange
    try (var mockedStatic = mockStatic(ValidateDataAgainstSchema.class)) {
      when(mockValidationResult.isValid()).thenReturn(true);
      mockedStatic
          .when(
              () ->
                  ValidateDataAgainstSchema.validate(
                      any(RecordTemplate.class),
                      any(ValidationOptions.class),
                      any(Validator.class)))
          .thenReturn(mockValidationResult);

      // Act
      RecordTemplateValidator.validateTrim(mockRecord, mockValidationFailureHandler);

      // Assert
      verify(mockValidationFailureHandler, never()).accept(any(ValidationResult.class));
    }
  }

  @Test
  public void testValidateTrim_WhenValidationFails_CallsFailureHandler() {
    // Arrange
    try (var mockedStatic = mockStatic(ValidateDataAgainstSchema.class)) {
      when(mockValidationResult.isValid()).thenReturn(false);
      mockedStatic
          .when(
              () ->
                  ValidateDataAgainstSchema.validate(
                      any(RecordTemplate.class),
                      any(ValidationOptions.class),
                      any(Validator.class)))
          .thenReturn(mockValidationResult);

      // Act
      RecordTemplateValidator.validateTrim(mockRecord, mockValidationFailureHandler);

      // Assert
      verify(mockValidationFailureHandler).accept(mockValidationResult);
    }
  }

  @Test
  public void testValidateWithCustomValidator_WhenValidationSucceeds_DoesNotCallFailureHandler() {
    // Arrange
    try (var mockedStatic = mockStatic(ValidateDataAgainstSchema.class)) {
      when(mockValidationResult.isValid()).thenReturn(true);
      mockedStatic
          .when(
              () ->
                  ValidateDataAgainstSchema.validate(
                      any(RecordTemplate.class),
                      any(ValidationOptions.class),
                      any(Validator.class)))
          .thenReturn(mockValidationResult);

      // Act
      RecordTemplateValidator.validate(mockRecord, mockValidationFailureHandler, mockValidator);

      // Assert
      verify(mockValidationFailureHandler, never()).accept(any(ValidationResult.class));
    }
  }

  @Test
  public void testValidateWithCustomValidator_WhenValidationFails_CallsFailureHandler() {
    // Arrange
    try (var mockedStatic = mockStatic(ValidateDataAgainstSchema.class)) {
      when(mockValidationResult.isValid()).thenReturn(false);
      mockedStatic
          .when(
              () ->
                  ValidateDataAgainstSchema.validate(
                      any(RecordTemplate.class),
                      any(ValidationOptions.class),
                      any(Validator.class)))
          .thenReturn(mockValidationResult);

      // Act
      RecordTemplateValidator.validate(mockRecord, mockValidationFailureHandler, mockValidator);

      // Assert
      verify(mockValidationFailureHandler).accept(mockValidationResult);
    }
  }

  @Test
  public void
      testValidateTrimWithCustomValidator_WhenValidationSucceeds_DoesNotCallFailureHandler() {
    // Arrange
    try (var mockedStatic = mockStatic(ValidateDataAgainstSchema.class)) {
      when(mockValidationResult.isValid()).thenReturn(true);
      mockedStatic
          .when(
              () ->
                  ValidateDataAgainstSchema.validate(
                      any(RecordTemplate.class),
                      any(ValidationOptions.class),
                      any(Validator.class)))
          .thenReturn(mockValidationResult);

      // Act
      RecordTemplateValidator.validateTrim(mockRecord, mockValidationFailureHandler, mockValidator);

      // Assert
      verify(mockValidationFailureHandler, never()).accept(any(ValidationResult.class));
    }
  }

  @Test
  public void testValidateTrimWithCustomValidator_WhenValidationFails_CallsFailureHandler() {
    // Arrange
    try (var mockedStatic = mockStatic(ValidateDataAgainstSchema.class)) {
      when(mockValidationResult.isValid()).thenReturn(false);
      mockedStatic
          .when(
              () ->
                  ValidateDataAgainstSchema.validate(
                      any(RecordTemplate.class),
                      any(ValidationOptions.class),
                      any(Validator.class)))
          .thenReturn(mockValidationResult);

      // Act
      RecordTemplateValidator.validateTrim(mockRecord, mockValidationFailureHandler, mockValidator);

      // Assert
      verify(mockValidationFailureHandler).accept(mockValidationResult);
    }
  }
}
