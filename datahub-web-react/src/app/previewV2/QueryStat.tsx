import React from 'react';
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
    return <Container>{formatNumber(queryCountLast30Days)} queries</Container>;
};

export default QueryStat;
