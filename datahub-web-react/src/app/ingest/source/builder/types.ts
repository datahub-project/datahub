import { IngestionSourceBuilderStep } from './steps';

/**
 * The size of the builder modal
 */
export enum ModalSize {
    SMALL = 800,
    LARGE = 1200,
}

/**
 * The default executor id used for ingestion
 */
export const DEFAULT_EXECUTOR_ID = 'default';

export interface SourceConfig {
    urn: string;
    name: string;
    displayName: string;
    docsUrl: string;
    description?: string;
    recipe: string;
}

/**
 * Props provided to each step as input.
 */
export type StepProps = {
    state: SourceBuilderState;
    updateState: (newState: SourceBuilderState) => void;
    goTo: (step: IngestionSourceBuilderStep) => void;
    prev?: () => void;
    submit: (shouldRun?: boolean) => void;
    cancel: () => void;
    ingestionSources: SourceConfig[];
};

export type StringMapEntryInput = {
    /**
     * The key of the map entry
     */
    key: string;

    /**
     * The value fo the map entry
     */
    value: string;
};

/**
 * The object represents the state of the Ingestion Source Builder form.
 */
export interface SourceBuilderState {
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
        recipe?: string;

        /**
         * Advanced: The id of the executor to be used to complete ingestion
         */
        executorId?: string | null;

        /**
         * Advanced: The version of the DataHub Ingestion Framework to use to perform ingestion
         */
        version?: string | null;

        /**
         * Advanced: Whether or not to run this ingestion source in debug mode
         */
        debugMode?: boolean | null;

        /**
         * Advanced: Extra arguments for the ingestion run.
         */
        extraArgs?: StringMapEntryInput[] | null;
    };
}
