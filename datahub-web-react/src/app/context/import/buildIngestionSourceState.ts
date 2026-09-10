import type { MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { yamlToJson } from '@app/ingestV2/source/utils';

type BuildIngestionSourceStateParams = {
    sourceType: string;
    displayName: string;
    recipeYaml: string;
};

export function buildIngestionSourceState({
    sourceType,
    displayName,
    recipeYaml,
}: BuildIngestionSourceStateParams): MultiStepSourceBuilderState {
    return {
        type: sourceType,
        name: displayName,
        config: {
            recipe: yamlToJson(recipeYaml),
        },
    };
}
