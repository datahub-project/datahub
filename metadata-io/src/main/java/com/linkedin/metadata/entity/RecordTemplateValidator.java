package com.linkedin.metadata.entity;

import com.linkedin.common.urn.UrnValidator;
import com.linkedin.data.schema.validation.CoercionMode;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public class RecordTemplateValidator {

    private static final ValidationOptions DEFAULT_VALIDATION_OPTIONS = new ValidationOptions(
            RequiredMode.CAN_BE_ABSENT_IF_HAS_DEFAULT,
            CoercionMode.NORMAL,
            UnrecognizedFieldMode.DISALLOW
    );

    private static final UrnValidator URN_VALIDATOR = new UrnValidator();

    /**
     * Validates a {@link RecordTemplate} and applies a function if validation fails
     *
     * @param record record to be validated.ailure.
     */
    public static void validate(RecordTemplate record, Consumer<ValidationResult> onValidationFailure) {
        final ValidationResult result = ValidateDataAgainstSchema.validate(
                record,
                DEFAULT_VALIDATION_OPTIONS,
                URN_VALIDATOR);
        if (!result.isValid()) {
            onValidationFailure.accept(result);
        }
    }

    private RecordTemplateValidator() {

    }
}
