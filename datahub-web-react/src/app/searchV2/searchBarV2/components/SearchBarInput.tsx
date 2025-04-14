import { InputRef } from 'antd';
import React, { forwardRef, useCallback, useState } from 'react';

import { CommandK } from '@app/searchV2/CommandK';
import { SearchBar } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

interface Props {
    value: string;
    onChange?: (event: React.ChangeEvent<HTMLInputElement>) => void;
    onSearch?: () => void;
    onFocus?: () => void;
    onBlur?: () => void;
    isDropdownOpened?: boolean;
    placeholder?: string;
    showCommandK?: boolean;
}

const SearchBarInput = forwardRef<InputRef, Props>(
    (
        {
            value,
            onChange,
            onSearch,
            onFocus,
            onBlur,
            isDropdownOpened,
            placeholder,
            showCommandK,
        },
        ref,
    ) => {
        const [isFocused, setIsFocused] = useState<boolean>(false);
        const isShowNavBarRedesign = useShowNavBarRedesign();

        const onFocusHandler = useCallback(() => {
            setIsFocused(true);
            onFocus?.();
        }, [onFocus]);

        const onBlurHandler = useCallback(() => {
            setIsFocused(false);
            onBlur?.();
        }, [onBlur]);

        return (

            <SearchBar
                bordered={false}
                placeholder={placeholder}
                onPressEnter={onSearch}
                value={value}
                onChange={(_, event) => onChange?.(event)}
                data-testid="search-input"
                onFocus={onFocusHandler}
                onBlur={onBlurHandler}
                allowClear={isDropdownOpened || isFocused}
                ref={ref}
                suffix={<>{(showCommandK && !isDropdownOpened && !isFocused && <CommandK />) || null}</>}
                width={isShowNavBarRedesign ? '592px' : '100%'}
            />
        );
    },
);

export default SearchBarInput;
