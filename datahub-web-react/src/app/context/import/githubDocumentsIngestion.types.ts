import type { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

export type GitHubDocumentsIngestionLocationState = {
    initialBuilderState?: MultiStepSourceBuilderState;
    initialStepIndex?: number;
};
