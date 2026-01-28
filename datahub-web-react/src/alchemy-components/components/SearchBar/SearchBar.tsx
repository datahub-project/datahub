import { InputProps, InputRef } from 'antd';
import React, { forwardRef } from 'react';

import { StyledSearchBar } from '@components/components/SearchBar/components';
import { SearchBarProps } from '@components/components/SearchBar/types';

import { Icon } from '@src/alchemy-components';

export const searchBarDefaults: SearchBarProps = {
    placeholder: 'Search...',
    value: '',
    width: '100%',
    height: '40px',
    allowClear: true,
};

export const SearchBar = forwardRef<InputRef, SearchBarProps & Omit<InputProps, 'onChange'>>(
    (
        {
            placeholder = searchBarDefaults.placeholder,
            value = searchBarDefaults.value,
            width = searchBarDefaults.width,
            height = searchBarDefaults.height,
            allowClear = searchBarDefaults.allowClear,
            clearIcon,
            forceUncontrolled = false,
            onCompositionStart,
            onCompositionEnd,
            onChange,
            ...props
        },
        ref,
    ) => {
        // Override value handling when forceUncontrolled is true
        const inputValue = forceUncontrolled ? undefined : value;

        return (
            <StyledSearchBar
                placeholder={placeholder}
                onChange={(e) => onChange?.(e.target.value, e)}
                value={inputValue}
                prefix={<Icon icon="MagnifyingGlass" source="phosphor" />}
                allowClear={clearIcon ? allowClear && { clearIcon } : allowClear}
                $width={width}
                $height={height}
                data-testid="search-bar-input"
                ref={ref}
                onCompositionStart={onCompositionStart}
                onCompositionEnd={onCompositionEnd}
                {...props}
            />
        );
    },
);
