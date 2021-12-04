/**
 * Steps of the Ingestion Source Builder flow.
 */
export enum IngestionSourceBuilderStep {
    SELECT_TEMPLATE = 'SELECT_TEMPLATE',
    DEFINE_RECIPE = 'DEFINE_RECIPE',
    CONFIGURE_SOURCE_TEMPLATE = 'CONFIGURE_SOURCE_TEMPLATE',
    CREATE_SCHEDULE = 'CREATE_SCHEDULE',
    NAME_SOURCE = 'NAME_SOURCE',
}

/**
 * Props provided to each step as input.
 */
export type StepProps = {
    state: BaseBuilderState;
    updateState: (newState: BaseBuilderState) => void;
    goTo: (step: IngestionSourceBuilderStep) => void;
    prev?: () => void;
    submit: () => void;
    cancel: () => void;
};

export enum SinkType {
    DATAHUB_REST,
}

/**
 * The object represents the state of the Ingestion Source Builder form.
 */
export interface BaseBuilderState {
    /**
     * The name of the new ingestion source
     */
    name?: string;

    /**
     * The type of the source itself, e.g. mysql, bigquery, bigquery-usage. Should match the recipe.
     */
    type?: string;

    /**
     * The schedule on which to execute the source (optional)
     */
    schedule?: {
        /**
         * The time at which the source should begin to be executed
         */
        startTimeMs?: number | null;

        /**
         * Abbreviated timezone at which the schedule should be executed
         */
        timezone?: string | null;

        /**
         * The inteval on which the source should be executed, represented as a cron string
         */
        interval?: string | null;
    } | null;

    /**
     * Specific configurations for executing the source recipe
     */
    config?: {
        /**
         * The raw recipe itself, represented as JSON. Expected to contain embedded secrets.
         */
        recipe: string;

        /**
         * Advanced: The id of the executor to be used to complete ingestion
         */
        executorId?: string | null;

        /**
         * Advanced: The version of the DataHub Ingestion Framework to use to perform ingestion
         */
        version?: string | null;
    };
}
