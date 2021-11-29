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
    prev: () => void;
    submit: () => void;
};

export enum SourceType {
    BIGQUERY,
}

export enum SinkType {
    DATAHUB_REST,
}

export interface BaseBuilderState {
    /**
     * The type of the builder
     */
    type?: 'recipe' | 'template';

    /**
     * The name of the new ingestion source
     */
    name?: string;

    /**
     * The schedule on which to execute the source (optional)
     */
    schedule?: {
        /**
         * The time at which the source should begin to be executed
         */
        startTimeMs: number;

        /**
         * The time at which the source should stop being executed
         */
        endTimeMs?: number;

        /**
         * The inteval on which the source should be executed, represented as a cron string
         */
        interval: string;
    };
}

export interface RecipeBuilderState extends BaseBuilderState {
    /**
     * The raw recipe itself, represented as JSON. Expected to contain embedded secrets.
     */
    recipe: string;
}

export interface TemplateBuilderState extends BaseBuilderState {
    /**
     * Parameters about the source itself
     */
    source: {
        /**
         * The template to use to create the Ingestion Source. Undefined / null means raw recipe builder.
         */
        type: SourceType;

        /**
         * The source-specific configurations. Specific shape depends on the "SourceType"
         */
        configs: any;
    };

    /**
     * Parameters about the sink
     */
    sink?: {
        /**
         * The type of the sink
         */
        type: SinkType;

        /**
         * The actor who should be used when ingesting. Should default to the system if none is provided.
         */
        actor?: string;
    };
}
