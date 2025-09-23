import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { formatNumber } from '@app/shared/formatNumber';

const Container = styled.div`
    color: ${REDESIGN_COLORS.FOUNDATION_BLUE_4};
    font-size: 12px;
    white-space: nowrap;
`;

interface Props {
    queryCountLast30Days?: number | null;
}

const QueryStat = ({ queryCountLast30Days }: Props) => {
    return <Container>{formatNumber(queryCountLast30Days)} queries</Container>;
};

export default QueryStat;
