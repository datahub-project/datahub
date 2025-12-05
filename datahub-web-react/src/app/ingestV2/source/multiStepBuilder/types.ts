import { Step } from '@app/sharedV2/forms/multiStepForm/types';

export interface IngestionSourceFormStep extends Step {
    hideRightPanel?: boolean;
    hideBottomPanel?: boolean;
}
