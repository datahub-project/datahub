import React from 'react';
import { Input } from '@components';
import styled from 'styled-components';
import { SelectSizeOptions } from '../types';

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
}

export default function DropdownSearchBar({ placeholder, value, size, onChange }: DropdownSearchBarProps) {
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
            />
        </SearchInputContainer>
    );
}
