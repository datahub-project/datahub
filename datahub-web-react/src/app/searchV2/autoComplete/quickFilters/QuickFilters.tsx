/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import QuickFilter from '@app/searchV2/autoComplete/quickFilters/QuickFilter';
import { useQuickFiltersContext } from '@providers/QuickFiltersContext';

const QuickFiltersWrapper = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    flex-flow: wrap;
`;

interface Props {
    searchQuery?: string;
    setIsDropdownVisible: React.Dispatch<React.SetStateAction<boolean>>;
}
export default function QuickFilters({ searchQuery, setIsDropdownVisible }: Props) {
    const { quickFilters } = useQuickFiltersContext();

    return (
        <QuickFiltersWrapper>
            {quickFilters?.map((quickFilter) => (
                <QuickFilter
                    key={quickFilter.value}
                    quickFilter={quickFilter}
                    searchQuery={searchQuery}
                    setIsDropdownVisible={setIsDropdownVisible}
                />
            ))}
        </QuickFiltersWrapper>
    );
}
