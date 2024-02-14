import {
    AssertionAction,
    AssertionType,
    AssertionEvaluationParametersType,
    DatasetFreshnessSourceType,
    DateInterval,
    EntityType,
    SchemaFieldDataType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    FreshnessFieldKind,
    DatasetFilterType,
    VolumeAssertionType,
    IncrementingSegmentFieldTransformerType,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionValueChangeType,
    DatasetVolumeSourceType,
    SqlAssertionType,
    FieldAssertionType,
    FieldTransformType,
    FieldValuesFailThresholdType,
    FieldMetricType,
    DatasetFieldAssertionSourceType,
} from '../../../../../../../../types.generated';

/**
 * The object represents the state of the Assertion Builder form.
 */
export interface AssertionMonitorBuilderState {
    /**
     * The urn of the entity associated with the monitor
     */
    entityUrn?: string | null;

    /**
     * The type of the entity associated with the monitor
     */
    entityType?: EntityType | null;

    /**
     * The platform associated with the entity. Used to enable and disable specific
     * features within the flow based on platform support.
     */
    platformUrn?: string | null;

    /**
     * The assertion definition itself.
     */
    assertion?: {
        /**
         * The type of the source itself, e.g. mysql, bigquery, bigquery-usage. Should match the recipe.
         */
        type?: AssertionType | null;

        /**
         * An optional human-readable description of the assertion
         */
        description?: string | null;

        /**
         * The schedule on which to execute the source (optional)
         */
        freshnessAssertion?: {
            /**
             * The type of the Freshness assertion
             */
            type?: FreshnessAssertionType | null;

            /**
             * The schedule defining the assertion itself.
             */
            schedule?: {
                /**
                 * The type of the schedule.
                 */
                type?: FreshnessAssertionScheduleType | null;

                /**
                 * A cron schedule definition
                 */
                cron?: {
                    /**
                     * The cron definition string
                     */
                    cron?: string | null;

                    /**
                     * The timezone in which the cron is defined
                     */
                    timezone?: string | null;

                    /**
                     * Optional start offset. If not provided, the window will be between
                     */
                    windowStartOffsetMs?: number | null;
                } | null;

                /**
                 * A fixed interval schedule definition
                 */
                fixedInterval?: {
                    /**
                     * The date interval
                     */
                    unit?: DateInterval;

                    /**
                     * The number of units
                     */
                    multiple?: number;
                } | null;
            } | null;

            /**
             * An optional filter used to further partition the dataset
             */
            filter?: {
                /**
                 * The filter type
                 */
                type?: DatasetFilterType | null;

                /**
                 * The raw query if using a SQL FilterType
                 */
                sql?: string | null;
            } | null;
        } | null;

        /**
         * SQL assertion configuration
         */
        sqlAssertion?: {
            /**
             * The type of the SQL assertion
             */
            type?: SqlAssertionType | null;

            /**
             * The SQL statement to execute
             */
            statement?: string | null;

            /**
             * The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.
             */
            changeType?: AssertionValueChangeType | null;

            /**
             * The operator you'd like to apply to the result of the SQL query
             */
            operator?: AssertionStdOperator | null;

            /**
             * The parameters for the SQL assertion
             */
            parameters?: {
                /**
                 * The value parameter of an assertion
                 */
                value?: {
                    /**
                     * The parameter value
                     */
                    value?: string | null;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType | null;
                } | null;

                /**
                 * The maxValue parameter of an assertion
                 */
                maxValue?: {
                    /**
                     * The parameter value
                     */
                    value?: string | null;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType | null;
                } | null;

                /**
                 * The minValue parameter of an assertion
                 */
                minValue?: {
                    /**
                     * The parameter value
                     */
                    value?: string | null;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType | null;
                } | null;
            } | null;
        };

        /**
         * Volume assertion configuration
         */
        volumeAssertion?: {
            /**
             * The type of the Volume assertion
             */
            type?: VolumeAssertionType | null;

            segment?: {
                field?: {
                    /**
                     * The path of the field
                     */
                    path?: string | null;

                    /**
                     * The DataHub type of the field
                     */
                    type?: string | null;

                    /**
                     * The native type of the field
                     */
                    nativeType?: string | null;
                } | null;

                transformer?: {
                    /**
                     * The type of the transformer
                     */
                    type?: IncrementingSegmentFieldTransformerType | null;

                    /**
                     * The native type of the transformer
                     */
                    nativeType?: string | null;
                } | null;
            } | null;

            /**
             * The parameters for the volume assertion
             */
            parameters?: {
                /**
                 * The value parameter of an assertion
                 */
                value?: {
                    /**
                     * The parameter value
                     */
                    value?: string | null;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType | null;
                } | null;

                /**
                 * The maxValue parameter of an assertion
                 */
                maxValue?: {
                    /**
                     * The parameter value
                     */
                    value?: string | null;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType | null;
                } | null;

                /**
                 * The minValue parameter of an assertion
                 */
                minValue?: {
                    /**
                     * The parameter value
                     */
                    value?: string | null;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType | null;
                } | null;
            } | null;

            /**
             * Required if type is 'ROW_COUNT_TOTAL'
             */
            rowCountTotal?: {
                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator | null;
            } | null;

            /**
             * Required if type is 'ROW_COUNT_CHANGE'
             */
            rowCountChange?: {
                /**
                 * The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.
                 */
                type?: AssertionValueChangeType | null;

                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator | null;
            } | null;

            /**
             * Required if type is 'INCREMENTING_SEGMENT_ROW_COUNT_TOTAL'
             */
            incrementingSegmentRowCountTotal?: {
                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator | null;
            } | null;

            /**
             * Required if type is 'INCREMENTING_SEGMENT_ROW_COUNT_CHANGE'
             */
            incrementingSegmentRowCountChange?: {
                /**
                 * The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.
                 */
                type?: AssertionValueChangeType | null;

                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator | null;
            } | null;

            /**
             * An optional filter used to further partition the dataset
             */
            filter?: {
                /**
                 * The filter type
                 */
                type?: DatasetFilterType | null;

                /**
                 * The raw query if using a SQL FilterType
                 */
                sql?: string | null;
            } | null;
        } | null;

        /**
         * Field assertion configuration
         */
        fieldAssertion?: {
            /**
             * The type of the Field assertion
             */
            type?: FieldAssertionType | null;

            /**
             * Required if type is 'FIELD_VALUES'
             */
            fieldValuesAssertion?: {
                /**
                 * The field under evaluation
                 */
                field?: {
                    /**
                     * The path of the field
                     */
                    path?: string | null;

                    /**
                     * The DataHub type of the field
                     */
                    type?: SchemaFieldDataType | null;

                    /**
                     * The native type of the field
                     */
                    nativeType?: string | null;
                } | null;

                /**
                 * An optional transform to apply to field values
                 */
                transform?: {
                    /**
                     * The type of the field transform
                     */
                    type?: FieldTransformType | null;
                } | null;

                /**
                 * The parameters for the field assertion
                 */
                parameters?: {
                    /**
                     * The value parameter of an assertion
                     */
                    value?: {
                        /**
                         * The parameter value
                         */
                        value?: string | null;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType | null;
                    } | null;

                    /**
                     * The maxValue parameter of an assertion
                     */
                    maxValue?: {
                        /**
                         * The parameter value
                         */
                        value?: string | null;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType | null;
                    } | null;

                    /**
                     * The minValue parameter of an assertion
                     */
                    minValue?: {
                        /**
                         * The parameter value
                         */
                        value?: string | null;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType | null;
                    } | null;
                } | null;

                /**
                 * Additional customization about when the assertion should be considered to fail
                 */
                failThreshold?: {
                    /**
                     * The type of failure threshold
                     */
                    type?: FieldValuesFailThresholdType | null;

                    /**
                     * The value of the threshold
                     */
                    value?: number | null;
                } | null;

                /**
                 * Whether to ignore or allow nulls when running the values assertion
                 */
                excludeNulls?: boolean | null;

                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator | null;
            } | null;

            /**
             * Required if type is 'FIELD_METRIC'
             */
            fieldMetricAssertion?: {
                /**
                 * The field under evaluation
                 */
                field?: {
                    /**
                     * The path of the field
                     */
                    path?: string | null;

                    /**
                     * The DataHub type of the field
                     */
                    type?: SchemaFieldDataType | null;

                    /**
                     * The native type of the field
                     */
                    nativeType?: string | null;
                } | null;

                /**
                 * The specific metric to assert against
                 */
                metric?: FieldMetricType | null;

                /**
                 * The predicate to evaluate against the metric for the field
                 */
                operator?: AssertionStdOperator | null;

                /**
                 * The parameters for the field assertion
                 */
                parameters?: {
                    /**
                     * The value parameter of an assertion
                     */
                    value?: {
                        /**
                         * The parameter value
                         */
                        value?: string | null;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType | null;
                    } | null;

                    /**
                     * The maxValue parameter of an assertion
                     */
                    maxValue?: {
                        /**
                         * The parameter value
                         */
                        value?: string | null;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType | null;
                    } | null;

                    /**
                     * The minValue parameter of an assertion
                     */
                    minValue?: {
                        /**
                         * The parameter value
                         */
                        value?: string | null;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType | null;
                    } | null;
                } | null;
            } | null;

            /**
             * An optional filter used to further partition the dataset
             */
            filter?: {
                /**
                 * The filter type
                 */
                type?: DatasetFilterType | null;

                /**
                 * The raw query if using a SQL FilterType
                 */
                sql?: string | null;
            } | null;
        } | null;

        /**
         * Configurations for actions to be performed on assertion success or failure.
         */
        actions?: {
            /**
             * Actions to run on assertion success.
             */
            onSuccess?: AssertionAction[] | null;

            /**
             * Actions to run on assertion failure.
             */
            onFailure?: AssertionAction[] | null;
        } | null;
    };

    /**
     * Configurations for the assertion evaluation schedule.
     */
    schedule?: {
        /**
         * A raw cron string
         */
        cron?: string | null;

        /**
         * The cron timezone.
         */
        timezone?: string | null;
    } | null;

    /**
     * Parameters for the assertion
     */
    parameters?: {
        /**
         * The type of the parameters
         */
        type?: AssertionEvaluationParametersType | null;

        /**
         * Information required to execute a dataset change operation Freshness assertion.
         */
        datasetFreshnessParameters?: {
            /**
             * The source type of the operation
             */
            sourceType?: DatasetFreshnessSourceType | null;

            /**
             * The field that should be used when determining whether an operation has occurred. Only applicable if source type = "FIELD_VALUE"
             */
            field?: {
                /**
                 * The urn of the field
                 */
                urn?: string | null;

                /**
                 * The path of the field
                 */
                path?: string | null;

                /**
                 * The DataHub type of the field
                 */
                type?: SchemaFieldDataType | null;

                /**
                 * The native type of the field
                 */
                nativeType?: string | null;

                /**
                 * The type of the field being used to verify the Freshness Assertion
                 */
                kind?: FreshnessFieldKind | null;
            } | null;

            /**
             * The Audit log operation that should be used when determining whether an operation has occurred. Only applicable if source type = "AUDIT_LOG"
             */
            auditLog?: {
                /**
                 * The operation types that should be monitored.
                 */
                operationTypes?: string[] | null;

                /**
                 * Optional: The native name of the user with which the operation should be associated.
                 */
                userName?: string | null;
            } | null;
        } | null;

        /**
         * Information required to execute a Volume assertion
         */
        datasetVolumeParameters?: {
            /**
             * The source type of the operation
             */
            sourceType?: DatasetVolumeSourceType | null;
        } | null;

        /**
         * Information required to execute a Field assertion
         */
        datasetFieldParameters?: {
            /**
             * The source type of the operation
             */
            sourceType?: DatasetFieldAssertionSourceType | null;

            /**
             * Required if sourceType is 'CHANGED_ROWS_QUERY'
             */
            changedRowsField?: {
                /**
                 * The path of the field
                 */
                path?: string | null;

                /**
                 * The DataHub type of the field
                 */
                type?: string | null;

                /**
                 * The native type of the field
                 */
                nativeType?: string | null;

                /**
                 * The type of the field being used to verify the Freshness Assertion
                 */
                kind?: FreshnessFieldKind | null;
            } | null;
        } | null;
    } | null;

    /**
     * Advanced: The executor ID of the remote monitor service, if any.
     */
    executorId?: string | null;
}

/**
 * Enabled steps in the builder flow.
 */
export enum AssertionBuilderStep {
    SELECT_TYPE = 'SELECT_TYPE',
    CONFIGURE_ASSERTION = 'CONFIGURE_ASSERTION',
    CONFIGURE_ACTIONS = 'CONFIGURE_ACTIONS',
}

/**
 * Props provided to each step as input.
 */
export type StepProps = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    goTo: (step: AssertionBuilderStep, type?: AssertionType) => void;
    prev?: () => void;
    submit: () => Promise<void>;
    cancel: () => void;
};

/**
 * State required to use the assertion actions builder.
 */
export interface AssertionActionsBuilderState {
    /**
     * Configurations for actions to be performed on assertion success or failure.
     */
    actions?: {
        /**
         * Actions to run on assertion success.
         */
        onSuccess?: AssertionAction[] | null;

        /**
         * Actions to run on assertion failure.
         */
        onFailure?: AssertionAction[] | null;
    } | null;
}

/**
 * State required to use the assertiona actions form.
 */
export type AssertionActionsFormState = {
    onFailure?: AssertionAction[] | null;
    onSuccess?: AssertionAction[] | null;
};
