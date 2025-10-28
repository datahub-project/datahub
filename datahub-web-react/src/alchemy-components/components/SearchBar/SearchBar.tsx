import { InputProps, InputRef } from 'antd';
import React, { forwardRef, useCallback, useEffect, useState } from 'react';
import { useDebounce } from 'react-use';

import { StyledSearchBar } from '@components/components/SearchBar/components';
import { SearchBarProps } from '@components/components/SearchBar/types';

import { Icon } from '@src/alchemy-components';

export const searchBarDefaults: SearchBarProps = {
    placeholder: 'Search...',
    value: '',
    width: '100%',
    height: '40px',
    allowClear: true,
    debounceDelay: 0,
};

export const SearchBar = forwardRef<InputRef, SearchBarProps & Omit<InputProps, 'onChange'>>(
    (
        {
            placeholder = searchBarDefaults.placeholder,
            value = searchBarDefaults.value,
            width = searchBarDefaults.width,
            height = searchBarDefaults.height,
            allowClear = searchBarDefaults.allowClear,
            debounceDelay = searchBarDefaults.debounceDelay,
            clearIcon,
            forceUncontrolled = false,
            onCompositionStart,
            onCompositionEnd,
            onChange,
            ...props
        },
        ref,
    ) => {
        // Local state to track the input value for immediate UI updates
        const [localValue, setLocalValue] = useState(value || '');

        // Update local value when controlled value prop changes
        useEffect(() => {
            if (!forceUncontrolled && value !== undefined) {
                setLocalValue(value);
            }
        }, [value, forceUncontrolled]);

        // Call onChange with debouncing when debounceDelay is set
        useDebounce(
            () => {
                if (onChange && debounceDelay !== undefined && debounceDelay > 0) {
                    // Create a synthetic event-like object for the onChange signature
                    const syntheticEvent = {
                        target: { value: localValue },
                    } as React.ChangeEvent<HTMLInputElement>;
                    onChange(localValue, syntheticEvent);
                }
            },
            debounceDelay,
            [localValue],
        );

        // Handle immediate onChange when debounceDelay is 0
        const handleChange = useCallback(
            (e: React.ChangeEvent<HTMLInputElement>) => {
                const newValue = e.target.value;
                setLocalValue(newValue);

                if (debounceDelay === 0) {
                    onChange?.(newValue, e);
                }
            },
            [onChange, debounceDelay],
        );

        // Override value handling when forceUncontrolled is true
        const inputValue = forceUncontrolled ? undefined : localValue;

        return (
            <StyledSearchBar
                placeholder={placeholder}
                onChange={handleChange}
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
