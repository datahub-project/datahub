import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Text } from '@src/alchemy-components';

import NoStatsAvailble from '@images/no-stats-available.svg?react';

const NoDataContainer = styled.div`
    margin: 40px auto;
    display: flex;
    flex-direction: column;
    align-items: center;
`;

const NoStatsIcon = styled(NoStatsAvailble)`
    width: 80px;
    height: 80px;
    margin-bottom: 6px;
    color: transparent;
`;

export default function NoStats() {
    const { t } = useTranslation('entity.profile.schema');

    return (
        <NoDataContainer>
            <NoStatsIcon />
            <Text size="sm">{t('statsV2.noColumnStatsFound')}</Text>
        </NoDataContainer>
    );
}
