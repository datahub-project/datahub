import type { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

export type IngestionSourceCreatePageLocationState = {
    initialBuilderState?: MultiStepSourceBuilderState;
    initialStepIndex?: number;
    backUrl?: string;
};
