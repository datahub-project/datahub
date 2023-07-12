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
     * The type of the entity associated with the monnitor
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
    } | null;
}

/**
 * Enabled steps in the builder flow.
 */
export enum AssertionBuilderStep {
    SELECT_TYPE = 'SELECT_TYPE',
    CONFIGURE_DATASET_FRESHNESS_ASSERTION = 'CONFIGURE_DATASET_FRESHNESS_ASSERTION',
    CONFIGURE_SCHEDULE = 'CONFIGURE_SCHEDULE',
}

/**
 * Props provided to each step as input.
 */
export type StepProps = {
    state: AssertionMonitorBuilderState;
    updateState: (newState: AssertionMonitorBuilderState) => void;
    goTo: (step: AssertionBuilderStep) => void;
    prev?: () => void;
    submit: () => void;
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
