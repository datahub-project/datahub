import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

import UserWithAvatar from '@app/ingestV2/shared/components/UserWithAvatar';
import { CLI_INGESTION_SOURCE, MANUAL_INGESTION_SOURCE, SCHEDULED_INGESTION_SOURCE } from '@app/ingestV2/source/utils';

import { CorpUser } from '@types';

const UserContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    wrap: auto;
`;

interface SourceColumnProps {
    source?: string | null;
    actor?: CorpUser | null;
}

export function ExecutedByColumn({ source, actor }: SourceColumnProps) {
    const { t } = useTranslation('ingestion');
    switch (source) {
        case MANUAL_INGESTION_SOURCE:
            if (!actor) return <>{t('executions.manualExecution')}</>;
            return (
                <UserContainer>
                    <Trans
                        i18nKey="executions.manualExecutionBy"
                        t={t}
                        components={{ user: <UserWithAvatar user={actor} /> }}
                    />
                </UserContainer>
            );

        case SCHEDULED_INGESTION_SOURCE:
            return <span>{t('executions.scheduledExecution')}</span>;

        case CLI_INGESTION_SOURCE:
            return <span>{t('executions.cliExecution')}</span>;

        default:
            return <span>-</span>;
    }
}
