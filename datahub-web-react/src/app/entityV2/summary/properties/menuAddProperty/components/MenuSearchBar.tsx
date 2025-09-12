import { SearchBar } from '@components';
import React from 'react';
import styled from 'styled-components';

const SearchBarWrapper = styled.div`
    padding-bottom: 4px;
`;

interface Props {
    value: string;
    onChange: (query: string) => void;
}

export default function MenuSearchBar({ value, onChange }: Props) {
    return (
        // wrapper to stop propagation of clicks to prevent closing of a menu
        <SearchBarWrapper onClick={(e) => e.stopPropagation()}>
            <SearchBar value={value} onChange={onChange} />
        </SearchBarWrapper>
    );
}
