import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EmptyMessageContainer } from '@src/alchemy-components/components/GraphCard/components';

const EmptyMessageWrapper = styled.div`
    text-align: center;
`;

interface Props {
    statName: string;
}

const NoPermission = ({ statName }: Props) => {
    const { t } = useTranslation('entity.profile.stats');
    return (
        <EmptyMessageContainer data-testid="no-permissions">
            <EmptyMessageWrapper>
                <Text size="2xl" weight="bold">
                    {t('noPermission.heading')}
                </Text>
                <Text>{t('noPermission.message', { statName })}</Text>
            </EmptyMessageWrapper>
        </EmptyMessageContainer>
    );
};

export default NoPermission;
