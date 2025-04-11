import React from 'react';
import styled from 'styled-components';
import { formatNumber } from '../shared/formatNumber';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';

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
