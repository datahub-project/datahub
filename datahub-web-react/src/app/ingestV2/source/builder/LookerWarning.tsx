import React from 'react';
import { Trans, useTranslation } from 'react-i18next';

import { Alert } from '@components/components/Alert';

import { LOOKER } from '@app/ingestV2/source/builder/constants';

const LOOKML_DOC_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/looker#module-lookml';
const LOOKER_DOC_LINK = 'https://docs.datahub.com/docs/generated/ingestion/sources/looker#module-looker';

interface Props {
    type: string;
}

type SourceLinkProps = {
    href: string;
    label: string;
    children?: React.ReactNode;
};

function SourceLink({ href, label, children }: SourceLinkProps) {
    return (
        <a href={href} target="_blank" rel="noopener noreferrer">
            {React.Children.count(children) ? children : label}
        </a>
    );
}

export const LookerWarning = ({ type }: Props) => {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const isLookerSource = type === LOOKER;
    const linkHref = isLookerSource ? LOOKML_DOC_LINK : LOOKER_DOC_LINK;
    const sourceName = isLookerSource ? t('looker.lookmlSourceLink') : t('looker.lookerSourceLink');

    return (
        <Alert
            style={{ marginBottom: '10px' }}
            variant="warning"
            title={
                <Trans
                    t={t}
                    i18nKey="looker.warning"
                    components={{
                        bold: <b />,
                        anchor: <SourceLink href={linkHref} label={sourceName} />,
                    }}
                />
            }
        />
    );
};
