import { Alert } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { LOOKER, LOOK_ML } from '@app/ingestV2/source/builder/constants';

const LOOKML_DOC_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/looker#module-lookml';
const LOOKER_DOC_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/looker#module-looker';

interface Props {
    type: string;
}

export const LookerWarning = ({ type }: Props) => {
    const { t } = useTranslation('ingestion.sourceBuilder');

    let link: React.ReactNode;
    if (type === LOOKER) {
        link = (
            <a href={LOOKML_DOC_LINK} target="_blank" rel="noopener noreferrer">
                {t('looker.lookmlSourceLink')}
            </a>
        );
    } else if (type === LOOK_ML) {
        link = (
            <a href={LOOKER_DOC_LINK} target="_blank" rel="noopener noreferrer">
                {t('looker.lookerSourceLink')}
            </a>
        );
    }

    return (
        <Alert
            style={{ marginBottom: '10px' }}
            type="warning"
            banner
            message={
                <Trans
                    t={t}
                    i18nKey="looker.warning"
                    components={{
                        bold: <b />,
                        anchor: <>{link}</>,
                    }}
                />
            }
        />
    );
};
