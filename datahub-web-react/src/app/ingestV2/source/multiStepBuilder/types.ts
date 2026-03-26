import { SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { Step } from '@app/sharedV2/forms/multiStepForm/types';

import { IngestionSource } from '@types';

export interface IngestionSourceFormStep extends Step {
    hideRightPanel?: boolean;
    hideBottomPanel?: boolean;
}

export interface MultiStepSourceBuilderState extends SourceBuilderState {
    shouldRun?: boolean;
    ingestionSource?: IngestionSource;
    isEditing?: boolean;
    // To restore last validation state after moving back
    isConnectionDetailsValid?: boolean;
}

export interface SubmitOptions {
    shouldRun?: boolean;
}
