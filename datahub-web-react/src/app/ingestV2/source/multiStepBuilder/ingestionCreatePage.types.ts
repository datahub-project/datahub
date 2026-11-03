import type { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

/** Location state for `/ingestion/create`. */
export type IngestionSourceCreatePageLocationState = {
    initialBuilderState?: MultiStepSourceBuilderState;
    initialStepIndex?: number;
    backUrl?: string;
};
