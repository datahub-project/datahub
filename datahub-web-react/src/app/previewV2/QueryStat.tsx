import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { formatNumber } from '@app/shared/formatNumber';

const Container = styled.div`
    color: ${(props) => props.theme.colors.textTertiary};
    font-size: 12px;
    white-space: nowrap;
`;

interface Props {
    queryCountLast30Days?: number | null;
}

const QueryStat = ({ queryCountLast30Days }: Props) => {
    const { t } = useTranslation('entity.preview');
    return (
        <Container>
            {t('queriesCount', {
                count: queryCountLast30Days ?? 0,
                formattedCount: formatNumber(queryCountLast30Days),
            })}
        </Container>
    );
};

export default QueryStat;
