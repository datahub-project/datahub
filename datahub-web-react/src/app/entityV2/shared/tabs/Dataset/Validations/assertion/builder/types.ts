import {
    AssertionAction,
    AssertionEvaluationParametersType,
    AssertionExclusionWindowType,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    AssertionValueChangeType,
    DatasetFieldAssertionSourceType,
    DatasetFilterType,
    DatasetFreshnessSourceType,
    DatasetVolumeSourceType,
    DateInterval,
    EntityType,
    FieldAssertionType,
    FieldMetricType,
    FieldTransformType,
    FieldValuesFailThresholdType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    FreshnessFieldKind,
    IncrementingSegmentFieldTransformerType,
    SchemaAssertionCompatibility,
    SchemaFieldDataType,
    SqlAssertionType,
    VolumeAssertionType,
} from '../../../../../../../../types.generated';

/**
 * Extending the graphql schedule type to include local options such as 'inferred'
 */
enum FreshnessAssertionBuilderExtraScheduleTypes {
    AiInferred = 'AI_INFERRED',
}
export const FreshnessAssertionScheduleBuilderTypeOptions = {
    ...FreshnessAssertionScheduleType,
    ...FreshnessAssertionBuilderExtraScheduleTypes,
};
export type FreshnessAssertionBuilderScheduleType =
    (typeof FreshnessAssertionScheduleBuilderTypeOptions)[keyof typeof FreshnessAssertionScheduleBuilderTypeOptions];

export type FreshnessAssertionBuilderSchedule = Required<
    Required<Required<AssertionMonitorBuilderState>['assertion']>['freshnessAssertion']
>['schedule'];
export type FreshnessAssertionBuilderFilter = Required<
    Required<Required<AssertionMonitorBuilderState>['assertion']>['freshnessAssertion']
>['filter'];

/**
 * Extending the graphql volume assertion type to include local options such as 'inferred'
 */
enum VolumeAssertionBuilderExtraTypes {
    AiInferredRowCountTotal = 'AI_INFERRED_ROW_COUNT_TOTAL',
}
export const VolumeAssertionBuilderTypeOptions = {
    ...VolumeAssertionBuilderExtraTypes,
    ...VolumeAssertionType,
};
export type VolumeAssertionBuilderType =
    (typeof VolumeAssertionBuilderTypeOptions)[keyof typeof VolumeAssertionBuilderTypeOptions];
export type VolumeAssertionBuilderState = Required<
    Required<AssertionMonitorBuilderState>['assertion']
>['volumeAssertion'];

/**
 * Extending the graphql field metric operator type to include local options such as 'inferred'
 */
enum FieldMetricAssertionBuilderExtraOperators {
    AiInferred = 'AI_INFERRED',
}
export const FieldMetricAssertionBuilderOperatorOptions = {
    ...FieldMetricAssertionBuilderExtraOperators,
    ...AssertionStdOperator,
};
export type FieldMetricAssertionBuilderOperator =
    (typeof FieldMetricAssertionBuilderOperatorOptions)[keyof typeof FieldMetricAssertionBuilderOperatorOptions];

/**
 * Exclusion window type for the builder
 */
export type AssertionMonitorBuilderExclusionWindow = Required<
    Required<AssertionMonitorBuilderState>['inferenceSettings']
>['exclusionWindows'];

/**
 * The object represents the state of the Assertion Builder form.
 */
export interface AssertionMonitorBuilderState {
    /**
     * The urn of the entity associated with the monitor
     */
    entityUrn?: string;

    /**
     * The type of the entity associated with the monitor
     */
    entityType?: EntityType;

    /**
     * The platform associated with the entity. Used to enable and disable specific
     * features within the flow based on platform support.
     */
    platformUrn?: string;

    /**
     * The assertion definition itself.
     */
    assertion?: {
        /**
         * The urn of the assertion. Only available if the assertion has been created and is being updated.
         */
        urn?: string;

        /**
         * The type of the source itself, e.g. mysql, bigquery, bigquery-usage. Should match the recipe.
         */
        type?: AssertionType;

        /**
         * An optional human-readable description of the assertion
         */
        description?: string;

        /**
         * The schedule on which to execute the source (optional)
         */
        freshnessAssertion?: {
            /**
             * The type of the Freshness assertion
             */
            type?: FreshnessAssertionType;

            /**
             * The schedule defining the assertion itself.
             */
            schedule?: {
                /**
                 * The type of the schedule.
                 */
                type?: FreshnessAssertionBuilderScheduleType;

                /**
                 * A cron schedule definition
                 */
                cron?: {
                    /**
                     * The cron definition string
                     */
                    cron?: string;

                    /**
                     * The timezone in which the cron is defined
                     */
                    timezone?: string;

                    /**
                     * Optional start offset. If not provided, the window will be between
                     */
                    windowStartOffsetMs?: number;
                };

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
                };
            };

            /**
             * An optional filter used to further partition the dataset
             */
            filter?: {
                /**
                 * The filter type
                 */
                type?: DatasetFilterType;

                /**
                 * The raw query if using a SQL FilterType
                 */
                sql?: string;
            };
        };

        /**
         * SQL assertion configuration
         */
        sqlAssertion?: {
            /**
             * The type of the SQL assertion
             */
            type?: SqlAssertionType;

            /**
             * The SQL statement to execute
             */
            statement?: string;

            /**
             * The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.
             */
            changeType?: AssertionValueChangeType;

            /**
             * The operator you'd like to apply to the result of the SQL query
             */
            operator?: AssertionStdOperator;

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
                    value?: string;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType;
                };

                /**
                 * The maxValue parameter of an assertion
                 */
                maxValue?: {
                    /**
                     * The parameter value
                     */
                    value?: string;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType;
                };

                /**
                 * The minValue parameter of an assertion
                 */
                minValue?: {
                    /**
                     * The parameter value
                     */
                    value?: string;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType;
                };
            };
        };

        /**
         * Volume assertion configuration
         */
        volumeAssertion?: {
            /**
             * The type of the Volume assertion
             */
            type?: VolumeAssertionBuilderType;

            segment?: {
                field?: {
                    /**
                     * The path of the field
                     */
                    path?: string;

                    /**
                     * The DataHub type of the field
                     */
                    type?: string;

                    /**
                     * The native type of the field
                     */
                    nativeType?: string;
                };

                transformer?: {
                    /**
                     * The type of the transformer
                     */
                    type?: IncrementingSegmentFieldTransformerType;

                    /**
                     * The native type of the transformer
                     */
                    nativeType?: string;
                };
            };

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
                    value?: string;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType;
                };

                /**
                 * The maxValue parameter of an assertion
                 */
                maxValue?: {
                    /**
                     * The parameter value
                     */
                    value?: string;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType;
                };

                /**
                 * The minValue parameter of an assertion
                 */
                minValue?: {
                    /**
                     * The parameter value
                     */
                    value?: string;

                    /**
                     * The type of the parameter
                     */
                    type?: AssertionStdParameterType;
                };
            };

            /**
             * Required if type is 'ROW_COUNT_TOTAL'
             */
            rowCountTotal?: {
                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator;
            };

            /**
             * Required if type is 'ROW_COUNT_CHANGE'
             */
            rowCountChange?: {
                /**
                 * The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.
                 */
                type?: AssertionValueChangeType;

                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator;
            };

            /**
             * Required if type is 'INCREMENTING_SEGMENT_ROW_COUNT_TOTAL'
             */
            incrementingSegmentRowCountTotal?: {
                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator;
            };

            /**
             * Required if type is 'INCREMENTING_SEGMENT_ROW_COUNT_CHANGE'
             */
            incrementingSegmentRowCountChange?: {
                /**
                 * The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.
                 */
                type?: AssertionValueChangeType;

                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator;
            };

            /**
             * An optional filter used to further partition the dataset
             */
            filter?: {
                /**
                 * The filter type
                 */
                type?: DatasetFilterType;

                /**
                 * The raw query if using a SQL FilterType
                 */
                sql?: string;
            };
        };

        /**
         * Field assertion configuration
         */
        fieldAssertion?: {
            /**
             * The type of the Field assertion
             */
            type?: FieldAssertionType;

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
                    path?: string;

                    /**
                     * The DataHub type of the field
                     */
                    type?: SchemaFieldDataType;

                    /**
                     * The native type of the field
                     */
                    nativeType?: string;
                };

                /**
                 * An optional transform to apply to field values
                 */
                transform?: {
                    /**
                     * The type of the field transform
                     */
                    type?: FieldTransformType;
                };

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
                        value?: string;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType;
                    };

                    /**
                     * The maxValue parameter of an assertion
                     */
                    maxValue?: {
                        /**
                         * The parameter value
                         */
                        value?: string;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType;
                    };

                    /**
                     * The minValue parameter of an assertion
                     */
                    minValue?: {
                        /**
                         * The parameter value
                         */
                        value?: string;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType;
                    };
                };

                /**
                 * Additional customization about when the assertion should be considered to fail
                 */
                failThreshold?: {
                    /**
                     * The type of failure threshold
                     */
                    type?: FieldValuesFailThresholdType;

                    /**
                     * The value of the threshold
                     */
                    value?: number;
                };

                /**
                 * Whether to ignore or allow nulls when running the values assertion
                 */
                excludeNulls?: boolean;

                /**
                 * A boolean operator that is applied on the input to an assertion
                 */
                operator?: AssertionStdOperator;
            };

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
                    path?: string;

                    /**
                     * The DataHub type of the field
                     */
                    type?: SchemaFieldDataType;

                    /**
                     * The native type of the field
                     */
                    nativeType?: string;
                };

                /**
                 * The specific metric to assert against
                 */
                metric?: FieldMetricType;

                /**
                 * The predicate to evaluate against the metric for the field
                 */
                operator?: FieldMetricAssertionBuilderOperator;

                /**
                 * The parameters for the field assertion
                 * Ignored for AI inferred assertions
                 */
                parameters?: {
                    /**
                     * The value parameter of an assertion
                     */
                    value?: {
                        /**
                         * The parameter value
                         */
                        value?: string;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType;
                    };

                    /**
                     * The maxValue parameter of an assertion
                     */
                    maxValue?: {
                        /**
                         * The parameter value
                         */
                        value?: string;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType;
                    };

                    /**
                     * The minValue parameter of an assertion
                     */
                    minValue?: {
                        /**
                         * The parameter value
                         */
                        value?: string;

                        /**
                         * The type of the parameter
                         */
                        type?: AssertionStdParameterType;
                    };
                };
            };

            /**
             * An optional filter used to further partition the dataset
             */
            filter?: {
                /**
                 * The filter type
                 */
                type?: DatasetFilterType;

                /**
                 * The raw query if using a SQL FilterType
                 */
                sql?: string;
            };
        };

        /**
         * Schema Field Assertion
         */
        schemaAssertion?: {
            /**
             * The type of the Field assertion
             */
            compatibility?: SchemaAssertionCompatibility;

            /**
             * Required if type is 'FIELD_VALUES'
             */
            fields?: {
                /**
                 * The V1 field path
                 */
                path?: string;

                /**
                 * The standard data type
                 */
                type?: SchemaFieldDataType;

                /**
                 * The native data type (optional)
                 */
                nativeType?: string;
            }[];
        };

        /**
         * Configurations for actions to be performed on assertion success or failure.
         */
        actions?: {
            /**
             * Actions to run on assertion success.
             */
            onSuccess?: AssertionAction[];

            /**
             * Actions to run on assertion failure.
             */
            onFailure?: AssertionAction[];
        };
    };

    /**
     * Settings for the AI assertion monitor.
     */
    inferenceSettings?: {
        /**
         * The exclusion windows for the assertion. If not provided, no exclusion windows are applied.
         */
        exclusionWindows?: {
            /**
             * The type of the exclusion window.
             */
            type: AssertionExclusionWindowType;

            /**
             * The display name for the exclusion window.
             */
            displayName?: string;

            /**
             * The fixed range for the exclusion window.
             */
            fixedRange?: {
                startTimeMillis: number;
                endTimeMillis: number;
            };

            /**
             * The holiday for the exclusion window.
             */
            holiday?: {
                name: string;
                region?: string;
                timezone?: string;
            };

            /**
             * The weekly window for the exclusion window.
             */
            weekly?: {
                daysOfWeek?: string[];
                startTime?: string;
                endTime?: string;
                timezone?: string;
            };
        }[];

        /**
         * The training lookback window in events. If not provided, the default is used.
         */
        trainingDataLookbackWindowDays?: number;

        /**
         * The sensitivity level for the assertion. If not provided, the default is applied.
         */
        sensitivity?: {
            /**
             * The level of sensitivity for the assertion.
             */
            level?: number;
        };
    };

    /**
     * Configurations for the assertion evaluation schedule.
     */
    schedule?: {
        /**
         * A raw cron string
         */
        cron: string;

        /**
         * The cron timezone.
         */
        timezone: string;
    };

    /**
     * Parameters for the assertion
     */
    parameters?: {
        /**
         * The type of the parameters
         */
        type?: AssertionEvaluationParametersType;

        /**
         * Information required to execute a dataset change operation Freshness assertion.
         */
        datasetFreshnessParameters?: {
            /**
             * The source type of the operation
             */
            sourceType?: DatasetFreshnessSourceType;

            /**
             * The field that should be used when determining whether an operation has occurred. Only applicable if source type = "FIELD_VALUE"
             */
            field?: {
                /**
                 * The urn of the field
                 */
                urn?: string;

                /**
                 * The path of the field
                 */
                path?: string;

                /**
                 * The DataHub type of the field
                 */
                type?: SchemaFieldDataType;

                /**
                 * The native type of the field
                 */
                nativeType?: string;

                /**
                 * The type of the field being used to verify the Freshness Assertion
                 */
                kind?: FreshnessFieldKind;
            };

            /**
             * The Audit log operation that should be used when determining whether an operation has occurred. Only applicable if source type = "AUDIT_LOG"
             */
            auditLog?: {
                /**
                 * The operation types that should be monitored.
                 */
                operationTypes?: string[];

                /**
                 * Optional: The native name of the user with which the operation should be associated.
                 */
                userName?: string;
            };
        };

        /**
         * Information required to execute a Volume assertion
         */
        datasetVolumeParameters?: {
            /**
             * The source type of the operation
             */
            sourceType?: DatasetVolumeSourceType;
        };

        /**
         * Information required to execute a Field assertion
         */
        datasetFieldParameters?: {
            /**
             * The source type of the operation
             */
            sourceType?: DatasetFieldAssertionSourceType;

            /**
             * Required if sourceType is 'CHANGED_ROWS_QUERY'
             */
            changedRowsField?: {
                /**
                 * The path of the field
                 */
                path?: string;

                /**
                 * The DataHub type of the field
                 */
                type?: string;

                /**
                 * The native type of the field
                 */
                nativeType?: string;

                /**
                 * The type of the field being used to verify the Freshness Assertion
                 */
                kind?: FreshnessFieldKind;
            };
        };
    };

    /**
     * Advanced: The executor ID of the remote monitor service, if any.
     */
    executorId?: string;
}

/**
 * Enabled steps in the builder flow.
 */
export enum AssertionBuilderStep {
    SELECT_TYPE = 'SELECT_TYPE',
    CONFIGURE_ASSERTION = 'CONFIGURE_ASSERTION',
    FINISH_UP = 'FINISH_UP',
    // CONFIGURE_ACTIONS = 'CONFIGURE_ACTIONS', REMOVED
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
 * State required to use the assertion actions form.
 */
export type AssertionActionsFormState = {
    onFailure?: AssertionAction[];
    onSuccess?: AssertionAction[];
};
