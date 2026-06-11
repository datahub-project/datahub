import { useTranslation } from 'react-i18next';

import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { CUSTOM_SOURCE_DISPLAY_NAME, getSourceConfigs } from '@app/ingestV2/source/utils';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

export function useScheduleStepSubtitle() {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { state } = useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();
    const { type } = state;
    const { ingestionSources } = useIngestionSources();
    const sourceConfigs = getSourceConfigs(ingestionSources, type as string);
    const sourceDisplayName = sourceConfigs?.displayName;

    const displayName =
        !sourceDisplayName || sourceDisplayName === CUSTOM_SOURCE_DISPLAY_NAME
            ? t('multiStep.connection.thisSource')
            : sourceDisplayName;

    return t('multiStep.schedule.subtitle', { displayName });
}
