import { Link } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { CUSTOM_SOURCE_DISPLAY_NAME, getSourceConfigs } from '@app/ingestV2/source/utils';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

export function ConnectionDetailsSubTitle() {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { state } = useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();
    const { type } = state;
    const { ingestionSources } = useIngestionSources();
    const sourceConfigs = getSourceConfigs(ingestionSources, type as string);
    const sourceDisplayName = sourceConfigs?.displayName;
    const docsUrl = sourceConfigs?.docsUrl;

    const isCustomSource = sourceDisplayName === CUSTOM_SOURCE_DISPLAY_NAME;
    const displayName = isCustomSource ? t('multiStep.connection.thisSource') : sourceDisplayName;
    const linkDisplayName = isCustomSource ? t('multiStep.connection.metadataIngestion') : displayName;

    return (
        <>
            {t('multiStep.connection.subtitle', { displayName })}
            {docsUrl && (
                <>
                    {' '}
                    <Trans
                        t={t}
                        i18nKey="multiStep.connection.subtitleDocs"
                        values={{ linkDisplayName }}
                        components={{
                            docsLink: <Link href={docsUrl}>{null}</Link>,
                        }}
                    />
                </>
            )}
        </>
    );
}
