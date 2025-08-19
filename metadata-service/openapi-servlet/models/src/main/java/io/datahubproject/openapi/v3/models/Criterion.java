package io.datahubproject.openapi.v3.models;

import com.linkedin.data.template.StringArray;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;
import java.util.List;

@Data
@Jacksonized
@Builder
public class Criterion {
    /**
     * The name of the field that the criterion refers to
     */
    private String field;

    /**
     * The value of the intended field
     */
    private String value;

    /**
     * Values. one of which the intended field should match
     * Note, if values is set, the above "value" field will be ignored
     */
    private List<String> values;

    /**
     * The condition for the criterion, e.g. EQUAL, START_WITH
     */
    @Builder.Default
    private Condition condition = Condition.EQUAL;

    /**
     * Whether the condition should be negated
     */
    private boolean negated;

    public enum Condition {
        EQUAL,
        STARTS_WITH,
        ENDS_WITH,
        EXISTS,
        IN
    }

    /**
     * Convert this criterion to its counterpart in RecordTemplate.
     */
    @Nullable
    public com.linkedin.metadata.query.filter.Criterion toRecordTemplate() {
        com.linkedin.metadata.query.filter.Criterion criterion = new com.linkedin.metadata.query.filter.Criterion();

        criterion.setField(this.field);
        criterion.setValue(this.value);

        if (this.values != null) {
            criterion.setValues(new StringArray(this.values));
        }

        criterion.setCondition(com.linkedin.metadata.query.filter.Condition.valueOf(this.condition.name()));
        criterion.setNegated(this.negated);
        return criterion;
    }
}
