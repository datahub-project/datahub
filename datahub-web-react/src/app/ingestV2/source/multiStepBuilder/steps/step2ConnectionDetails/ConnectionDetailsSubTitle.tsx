import { getSourceConfigs } from '@app/ingest/source/utils';
import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '../../types';

export function ConnectionDetailsSubTitle() {
    const { state } = useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();
    const { type } = state;
    const { ingestionSources } = useIngestionSources();
    const sourceConfigs = getSourceConfigs(ingestionSources, type as string);
    const sourceDisplayName = sourceConfigs?.displayName;

    return <>To import from {sourceDisplayName}, we'll need more information to connect to your instance.</>;
}
