import React from 'react';
import { Alert } from 'antd';
import { useTranslation } from 'react-i18next';
import { LOOKER, LOOK_ML } from './constants';

const LOOKML_DOC_LINK = 'https://datahubproject.io/docs/generated/ingestion/sources/looker#module-lookml';
const LOOKER_DOC_LINK = 'https://datahubproject.io/docs/generated/ingestion/sources/looker#module-looker';

interface Props {
    type: string;
}

export const LookerWarning = ({ type }: Props) => {
    const { t } = useTranslation();
    let link: React.ReactNode;
    if (type === LOOKER) {
        link = (
            <a href={LOOKML_DOC_LINK} target="_blank" rel="noopener noreferrer">
                {t('ingest.datahubLookMLIngestionSource')}
            </a>
        );
    } else if (type === LOOK_ML) {
        link = (
            <a href={LOOKER_DOC_LINK} target="_blank" rel="noopener noreferrer">
                {t('ingest.datahubLookerIngestionSource')}
            </a>
        );
    }

    return (
        <Alert
            style={{ marginBottom: '10px' }}
            type="warning"
            banner
            message={<>{t('ingest.toCompleteTheLookerIntegrationTextWithLink_component', { link })}</>}
        />
    );
};
