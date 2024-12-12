import { SearchOutlined } from '@ant-design/icons';
import React from 'react';
import { StyledSearchBar } from './components';
import { SearchBarProps } from './types';

export const searchBarDefaults: SearchBarProps = {
    placeholder: 'Search..',
    value: '',
    width: '272px',
    allowClear: true,
};

export const SearchBar = ({
    placeholder = searchBarDefaults.placeholder,
    value = searchBarDefaults.value,
    width = searchBarDefaults.width,
    allowClear = searchBarDefaults.allowClear,
    onChange,
}: SearchBarProps) => {
    return (
        <StyledSearchBar
            placeholder={placeholder}
            onChange={(e) => onChange?.(e.target.value)}
            value={value}
            prefix={<SearchOutlined />}
            allowClear={allowClear}
            $width={width}
        />
    );
};
