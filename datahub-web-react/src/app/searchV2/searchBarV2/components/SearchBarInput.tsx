import { CloseCircleFilled, SearchOutlined } from '@ant-design/icons';
import { Input, InputRef } from 'antd';
import React, { forwardRef, useCallback, useState } from 'react';
import styled from 'styled-components';

import { CommandK } from '@app/searchV2/CommandK';
import { colors } from '@src/alchemy-components';
import { ANTD_GRAY_V2, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const StyledSearchBar = styled(Input)<{
    $textColor?: string;
    $placeholderColor?: string;
    viewsEnabled?: boolean;
    $isShowNavBarRedesign?: boolean;
}>`
    &&& {
        border-radius: 8px;
        height: 40px;
        font-size: 14px;
        color: #dcdcdc;
        background-color: ${ANTD_GRAY_V2[2]};
        border: 2px solid transparent;
        padding-right: 2.5px;
        ${(props) =>
            !props.viewsEnabled &&
            `
        &:focus-within {
            border-color: ${props.theme.styles['primary-color']};
        }`}

        ${(props) => props.$isShowNavBarRedesign && 'width: 592px;'}
    }

    > .ant-input::placeholder {
        color: ${(props) =>
            props.$placeholderColor || (props.$isShowNavBarRedesign ? REDESIGN_COLORS.GREY_300 : '#dcdcdc')};
    }

    > .ant-input {
        color: ${(props) => props.$textColor || (props.$isShowNavBarRedesign ? '#000' : '#fff')};
    }

    .ant-input-clear-icon {
        height: 15px;
        width: 15px;
    }
`;

const SearchIcon = styled(SearchOutlined)<{ $isShowNavBarRedesign?: boolean }>`
    color: ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1800] : '#dcdcdc')};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        && svg {
            width: 16px;
            height: 16px;
        }
    `}
`;

const ClearIcon = styled(CloseCircleFilled)`
    svg {
        height: 15px;
        width: 15px;
    }
`;

interface Props {
    value: string;
    onChange?: (event: React.ChangeEvent<HTMLInputElement>) => void;
    onSearch?: () => void;
    onFocus?: () => void;
    onBlur?: () => void;
    isDropdownOpened?: boolean;
    placeholder?: string;
    style?: React.CSSProperties;
    textColor?: string;
    placeholderColor?: string;
    viewsEnabled?: boolean;
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
            style,
            textColor,
            placeholderColor,
            viewsEnabled,
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
            <StyledSearchBar
                bordered={false}
                placeholder={placeholder}
                onPressEnter={onSearch}
                style={{ ...(style || {}), color: '#fff' }}
                value={value}
                onChange={onChange}
                data-testid="search-input"
                onFocus={onFocusHandler}
                onBlur={onBlurHandler}
                viewsEnabled={viewsEnabled}
                $isShowNavBarRedesign={isShowNavBarRedesign}
                allowClear={((isDropdownOpened || isFocused) && { clearIcon: <ClearIcon /> }) || false}
                prefix={
                    <>
                        <SearchIcon $isShowNavBarRedesign={isShowNavBarRedesign} onClick={onSearch} />
                    </>
                }
                ref={ref}
                suffix={<>{(showCommandK && !isDropdownOpened && !isFocused && <CommandK />) || null}</>}
                $textColor={textColor}
                $placeholderColor={placeholderColor}
                width={isShowNavBarRedesign ? '592px' : '100%'}
            />
        );
    },
);

export default SearchBarInput;
