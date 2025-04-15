import { colors, radius, SearchBar, transition } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { InputRef } from 'antd';
import React, { forwardRef, useCallback, useState } from 'react';

import { CommandK } from '@app/searchV2/CommandK';
import { ViewSelect } from '@src/app/entityV2/view/select/ViewSelect';
import styled from 'styled-components';
import { BOX_SHADOW } from '../constants';

const PRE_NAV_BAR_REDESIGN_SEARCHBAR_BACKGROUND = '#343444';

const StyledSearchBar = styled(SearchBar)<{ $isShowNavBarRedesign?: boolean }>`
    border-width: 2px !important;
    border-color: ${colors.gray[100]};

    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        background: ${PRE_NAV_BAR_REDESIGN_SEARCHBAR_BACKGROUND};
        border-color: ${PRE_NAV_BAR_REDESIGN_SEARCHBAR_BACKGROUND};

        &:hover,
        &:focus,
        &:focus-within {
            border-color: ${props.theme.styles['primary-color']} !important;
        }

        .ant-input, .ant-input-clear-icon {
            color: ${colors.white};
            background: ${PRE_NAV_BAR_REDESIGN_SEARCHBAR_BACKGROUND};
        }
    `}
`;

const ViewSelectContainer = styled.div``;

export const Wrapper = styled.div<{ $open?: boolean; $isShowNavBarRedesign?: boolean }>`
    background: transparent;

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        padding: ${radius.md};
        transition: all ${transition.easing['ease-in']} ${transition.duration.slow};
        border-radius: ${radius.lg} ${radius.lg} ${radius.none} ${radius.none};
    `}

    ${(props) =>
        props.$open &&
        props.$isShowNavBarRedesign &&
        `
        background: ${colors.gray[1500]};
        box-shadow: ${BOX_SHADOW};
    `}
`;

const SuffixWrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 4px;
    padding: 4px 0 4px 0;
    line-height: 20px;
`;

interface Props {
    value: string;
    onChange?: (event: React.ChangeEvent<HTMLInputElement>) => void;
    onSearch?: () => void;
    onFocus?: () => void;
    onBlur?: () => void;
    isDropdownOpened?: boolean;
    placeholder?: string;
    showCommandK?: boolean;
    viewsEnabled?: boolean;
}

const SearchBarInput = forwardRef<InputRef, Props>(
    (
        { value, onChange, onSearch, onFocus, onBlur, isDropdownOpened, placeholder, showCommandK, viewsEnabled },
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
            <Wrapper $open={isDropdownOpened} $isShowNavBarRedesign={isShowNavBarRedesign}>
                <StyledSearchBar
                    placeholder={placeholder}
                    onPressEnter={onSearch}
                    value={value}
                    onChange={(_, event) => onChange?.(event)}
                    data-testid="search-input"
                    onFocus={onFocusHandler}
                    onBlur={onBlurHandler}
                    allowClear={isDropdownOpened || isFocused}
                    ref={ref}
                    suffix={
                        <SuffixWrapper>
                            {(showCommandK && !isDropdownOpened && !isFocused && <CommandK />) || null}
                            {viewsEnabled && (
                                <ViewSelectContainer onClick={(e) => e.stopPropagation()}>
                                    <ViewSelect />
                                </ViewSelectContainer>
                            )}
                        </SuffixWrapper>
                    }
                    width={isShowNavBarRedesign ? '664px' : '620px'}
                    height="44px"
                    $isShowNavBarRedesign={isShowNavBarRedesign}
                />
            </Wrapper>
        );
    },
);

export default SearchBarInput;
