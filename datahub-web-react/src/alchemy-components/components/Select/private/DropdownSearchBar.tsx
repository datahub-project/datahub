/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Input } from '@components';
import React from 'react';
import styled from 'styled-components';

import { SelectSizeOptions } from '@components/components/Select/types';

const SearchInputContainer = styled.div({
    position: 'relative',
    width: '100%',
    display: 'flex',
    justifyContent: 'center',
});

interface DropdownSearchBarProps {
    placeholder?: string;
    value?: string;
    size?: SelectSizeOptions;
    onChange?: (value: string) => void;
    onClear?: () => void;
}

export default function DropdownSearchBar({ placeholder, value, size, onChange, onClear }: DropdownSearchBarProps) {
    return (
        <SearchInputContainer>
            <Input
                label=""
                type="text"
                icon={{ icon: 'MagnifyingGlass', source: 'phosphor' }}
                placeholder={placeholder || 'Search...'}
                value={value}
                onChange={(e) => onChange?.(e.target.value)}
                style={{ fontSize: size || 'md' }}
                inputTestId="dropdown-search-input"
                onClear={onClear}
                data-testid="dropdown-search-bar"
            />
        </SearchInputContainer>
    );
}
