import { Alert } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

const CSV_FORMAT_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/csv-enricher';

export const CSVInfo = () => {
    const { t } = useTranslation('ingestion.sourceBuilder');

    return (
        <Alert
            style={{ marginBottom: '10px' }}
            type="warning"
            banner
            message={
                <Trans
                    t={t}
                    i18nKey="csv.message"
                    components={{
                        anchor: (
                            <a href={CSV_FORMAT_LINK} target="_blank" rel="noopener noreferrer">
                                {t('csv.linkText')}
                            </a>
                        ),
                    }}
                />
            }
        />
    );
};
