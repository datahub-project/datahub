import React from 'react';
import styled from 'styled-components';

import { Pill } from '@src/alchemy-components';
import { formatNumber } from '@src/app/shared/formatNumber';

const Container = styled.div`
    display: flex;
    align-items: center;
`;

const TabName = styled.div`
    margin-right: 8px;
`;

type Props = {
    name: string;
    count: number;
    loading: boolean;
};

const TabNameWithCount = ({ name, count = 0, loading }: Props) => {
    return (
        <Container>
            <TabName>{name}</TabName>
            {!loading && <Pill label={formatNumber(count)} size="sm" />}
        </Container>
    );
};
export default TabNameWithCount;
