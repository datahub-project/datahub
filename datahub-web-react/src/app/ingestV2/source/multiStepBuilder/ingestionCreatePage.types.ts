import type { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

/** Location state for `/ingestion/create` when deep-linking with a pre-filled recipe. */
export type IngestionSourceCreatePageLocationState = {
    initialBuilderState?: MultiStepSourceBuilderState;
    initialStepIndex?: number;
};
