<<<<<<< HEAD
import { MagnifyingGlass } from '@phosphor-icons/react';
||||||| 952f3cc3118
import { SearchOutlined } from '@ant-design/icons';
=======
import { Icon } from '@src/alchemy-components';
import { InputProps } from 'antd';
>>>>>>> master
import React from 'react';
import { StyledSearchBar } from './components';
import { SearchBarProps } from './types';

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
<<<<<<< HEAD
            prefix={<MagnifyingGlass />}
||||||| 952f3cc3118
            prefix={<SearchOutlined />}
=======
            prefix={<Icon icon="MagnifyingGlass" source="phosphor" />}
>>>>>>> master
            allowClear={allowClear}
            $width={width}
            data-testid="search-bar-input"
            {...props}
        />
    );
};
