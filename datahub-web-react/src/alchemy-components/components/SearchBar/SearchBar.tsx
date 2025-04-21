import { InputProps } from 'antd';
import React from 'react';

import { StyledSearchBar } from '@components/components/SearchBar/components';
import { SearchBarProps } from '@components/components/SearchBar/types';

import { Icon } from '@src/alchemy-components';

export const searchBarDefaults: SearchBarProps = {
    placeholder: 'Search...',
    value: '',
    width: '100%',
    allowClear: true,
};

export const SearchBar = ({
    placeholder = searchBarDefaults.placeholder,
    value = searchBarDefaults.value,
    width = searchBarDefaults.width,
    allowClear = searchBarDefaults.allowClear,
    onChange,
    ...props
}: SearchBarProps & Omit<InputProps, 'onChange'>) => {
    return (
        <StyledSearchBar
            placeholder={placeholder}
            onChange={(e) => onChange?.(e.target.value)}
            value={value}
            prefix={<Icon icon="MagnifyingGlass" source="phosphor" />}
            allowClear={allowClear}
            $width={width}
            data-testid="search-bar-input"
            {...props}
        />
    );
};
