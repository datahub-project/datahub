import { Link } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

export default function SelectSourceSubtitle() {
    const { t } = useTranslation('ingestion.sourceBuilder');
    return (
        <Trans
            t={t}
            i18nKey="multiStep.selectSource.subtitle"
            components={{
                docsLink: (
                    <Link
                        href="https://docs.datahub.com/docs/metadata-ingestion-security"
                        style={{ fontStyle: 'italic' }}
                    >
                        {null}
                    </Link>
                ),
            }}
        />
    );
}
