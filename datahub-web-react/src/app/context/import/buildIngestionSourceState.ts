import type { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';

type BuildIngestionSourceStateParams = {
    sourceType: string;
    displayName: string;
    recipe: string;
};

export function buildIngestionSourceState({
    sourceType,
    displayName,
    recipe,
}: BuildIngestionSourceStateParams): MultiStepSourceBuilderState {
    return {
        type: sourceType,
        name: displayName,
        config: {
            recipe,
        },
    };
}
