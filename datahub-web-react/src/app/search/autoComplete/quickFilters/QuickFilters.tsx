import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useAppStateContext } from '../../../../providers/AppStateContext';
import QuickFilter from './QuickFilter';

const QuickFiltersWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    flex-flow: wrap;
`;

export default function QuickFilters() {
    const { quickFilters } = useAppStateContext();

    return (
        <QuickFiltersWrapper>
            {quickFilters?.map((quickFilter) => (
                <QuickFilter quickFilter={quickFilter} />
            ))}
        </QuickFiltersWrapper>
    );
}
