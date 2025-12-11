/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SearchBar } from '@components';
import React from 'react';
import styled from 'styled-components';

const SearchBarWrapper = styled.div`
    padding-bottom: 4px;
`;

interface Props {
    value: string;
    onChange: (query: string) => void;
    dataTestId?: string;
}

export default function MenuSearchBar({ value, onChange, dataTestId }: Props) {
    return (
        // wrapper to stop propagation of clicks to prevent closing of a menu
        <SearchBarWrapper onClick={(e) => e.stopPropagation()} data-testid={dataTestId}>
            <SearchBar value={value} onChange={onChange} />
        </SearchBarWrapper>
    );
}
