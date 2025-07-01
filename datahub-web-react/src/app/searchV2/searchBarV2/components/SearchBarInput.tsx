import { InputRef } from 'antd';
import React, { forwardRef, useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { V2_SEARCH_BAR_VIEWS } from '@app/onboarding/configV2/HomePageOnboardingConfig';
import { CommandK } from '@app/searchV2/CommandK';
import { BOX_SHADOW } from '@app/searchV2/searchBarV2/constants';
import { Icon, SearchBar, colors, radius, transition } from '@src/alchemy-components';
import { ViewSelect } from '@src/app/entityV2/view/select/ViewSelect';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

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
    width: 100%;
    min-width: 500px;

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
    onViewsClick?: () => void;
    onClear?: () => void;
    isDropdownOpened?: boolean;
    placeholder?: string;
    showCommandK?: boolean;
    viewsEnabled?: boolean;
    width?: string;
}

const SearchBarInput = forwardRef<InputRef, Props>(
    (
        {
            value,
            onChange,
            onSearch,
            onFocus,
            onBlur,
            onViewsClick,
            onClear,
            isDropdownOpened,
            placeholder,
            showCommandK,
            viewsEnabled,
            width,
        },
        ref,
    ) => {
        const [isFocused, setIsFocused] = useState<boolean>(false);
        const [isViewsSelectOpened, setIsViewsSelectOpened] = useState<boolean>(false);
        const isShowNavBarRedesign = useShowNavBarRedesign();

        const onFocusHandler = useCallback(() => {
            setIsFocused(true);
            onFocus?.();
        }, [onFocus]);

        const onBlurHandler = useCallback(() => {
            setIsFocused(false);
            onBlur?.();
        }, [onBlur]);

        const onKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>) => {
            // disable changing position of cursor by keys used to change selected item in the search bar's response
            if (['ArrowUp', 'ArrowDown'].includes(e.key)) {
                e.preventDefault();
            }
        }, []);

        const onViewSelectContainerClickHandler = (event: React.MouseEvent) => {
            event.stopPropagation(); // do not open the autocomplete's dropdown by clicking on theviews button
        };

        const onViewsClickHandler = (isOpen: boolean) => {
            setIsViewsSelectOpened(isOpen);
            onViewsClick?.();
        };

        useEffect(() => {
            // Automatically close the views select when the autocomplete's dropdown is opened event by keayboard shortcut
            if (isDropdownOpened) setIsViewsSelectOpened(false);
        }, [isDropdownOpened]);

        return (
            <Wrapper $open={isDropdownOpened} $isShowNavBarRedesign={isShowNavBarRedesign}>
                <StyledSearchBar
                    placeholder={placeholder}
                    onPressEnter={onSearch}
                    onKeyDown={onKeyDown}
                    value={value}
                    onChange={(_, event) => onChange?.(event)}
                    data-testid="search-input"
                    onFocus={onFocusHandler}
                    onBlur={onBlurHandler}
                    allowClear={isDropdownOpened || isFocused}
                    clearIcon={<Icon onClick={onClear} icon="XCircle" source="phosphor" size="2xl" />}
                    ref={ref}
                    suffix={
                        <SuffixWrapper>
                            {(showCommandK && !isDropdownOpened && !isFocused && <CommandK />) || null}
                            {viewsEnabled && (
                                <ViewSelectContainer
                                    onClick={onViewSelectContainerClickHandler}
                                    id={V2_SEARCH_BAR_VIEWS}
                                >
                                    <ViewSelect isOpen={isViewsSelectOpened} onOpenChange={onViewsClickHandler} />
                                </ViewSelectContainer>
                            )}
                        </SuffixWrapper>
                    }
                    width={width ?? (isShowNavBarRedesign ? '664px' : '620px')}
                    height="44px"
                    $isShowNavBarRedesign={isShowNavBarRedesign}
                />
            </Wrapper>
        );
    },
);

export default SearchBarInput;
