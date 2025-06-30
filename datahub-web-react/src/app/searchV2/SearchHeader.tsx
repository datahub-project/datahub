import { ArrowRight } from '@phosphor-icons/react';
import { Button, Layout } from 'antd';
import React, { useContext, useState } from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import NavBarToggler from '@app/homeV2/layout/navBarRedesign/NavBarToggler';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { V2_SEARCH_BAR_ID } from '@app/onboarding/configV2/HomePageOnboardingConfig';
import { SearchBar } from '@app/searchV2/SearchBar';
import { SearchBarV2 } from '@app/searchV2/searchBarV2/SearchBarV2';
import useSearchViewAll from '@app/searchV2/useSearchViewAll';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { EntityRegistry } from '@src/entityRegistryContext';

import { AutoCompleteResultForEntity } from '@types';

const getStyles = ($isShowNavBarRedesign?: boolean) => {
    return {
        input: {
            backgroundColor: $isShowNavBarRedesign ? 'white' : '#343444',
        },
        searchBox: {
            maxWidth: $isShowNavBarRedesign ? '100%' : 620,
            minWidth: $isShowNavBarRedesign ? 300 : 400,
        },
        searchBoxContainer: {
            padding: 0,
            display: 'flex',
            justifyContent: 'center',
            width: $isShowNavBarRedesign ? '648px' : '620px',
            minWidth: '400px',
        },
    };
};

const Wrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    position: fixed;
    width: 100%;
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        line-height: 20px;
        padding: 0px 12px;
    `}
`;

const Header = styled(Layout)<{ $isNavBarCollapsed?: boolean; $isShowNavBarRedesign?: boolean }>`
    background-color: transparent;
    height: ${(props) => (props.$isShowNavBarRedesign ? '56px' : '72px')};
    display: flex;
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin-top: 8px;
        gap: 16px;
        flex-direction: row;

        // preventing of NavBar's overlapping
        position: relative;
        padding-left: ${props.$isNavBarCollapsed ? '224px' : '540px'};
        left: ${props.$isNavBarCollapsed ? '-112px' : '-270px'};
        transition: none;
        @media only screen and (min-width: 1280px) {
            padding-left: 540px;
            left: -270px;
        }
        @media only screen and (max-width: 1200px) {
            transition: padding 250ms ease-in-out;
        }
    `}
    ${(props) => props.$isShowNavBarRedesign && !props.$isNavBarCollapsed && 'justify-content: space-between;'}
    align-items: center;
`;

const HeaderBackground = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) => !props.$isShowNavBarRedesign && 'background-color: #171723;'}
    position: fixed;
    height: 100px;
    width: 100%;
    z-index: -1;
`;

const SearchBarContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex: 1;
    align-items: center;
    justify-content: center;
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        margin-left: 80px;
        margin-top: 6px;
    `}
`;

const StyledButton = styled(Button)`
    color: ${REDESIGN_COLORS.BODY_TEXT_GREY};
    text-align: center;

    font-family: Mulish;
    font-size: 12px;
    font-style: normal;
    font-weight: 700;
    line-height: normal;

    display: flex;
    gap: 4px;
    align-items: center;

    &:hover,
    :active,
    :focus {
        color: ${REDESIGN_COLORS.GREY_300};
    }
`;

const NavBarTogglerWrapper = styled.div`
    position: fixed;
    left: 68px;
`;

type Props = {
    initialQuery: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string) => void;
    onQueryChange: (query: string) => void;
    entityRegistry: EntityRegistry;
};

/**
 * A header containing a Logo, Search Bar view, & an account management dropdown.
 */
export const SearchHeader = ({
    initialQuery,
    placeholderText,
    suggestions,
    onSearch,
    onQueryChange,
    entityRegistry,
}: Props) => {
    const [, setIsSearchBarFocused] = useState(false);
    const appConfig = useAppConfig();
    const viewsEnabled = appConfig.config?.viewsConfig?.enabled || false;
    const { isUserInitializing } = useContext(OnboardingContext);
    const { isCollapsed } = useNavBarContext();
    const searchViewAll = useSearchViewAll();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const styles = getStyles(isShowNavBarRedesign);

    const showSearchBarAutocompleteRedesign = appConfig.config.featureFlags?.showSearchBarAutocompleteRedesign;
    const FinalSearchBar = showSearchBarAutocompleteRedesign ? SearchBarV2 : SearchBar;

    return (
        <>
            <HeaderBackground $isShowNavBarRedesign={isShowNavBarRedesign} />
            <Wrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                <Header $isShowNavBarRedesign={isShowNavBarRedesign} $isNavBarCollapsed={isCollapsed}>
                    {isShowNavBarRedesign && isCollapsed && (
                        <NavBarTogglerWrapper>
                            <NavBarToggler />
                        </NavBarTogglerWrapper>
                    )}
                    <SearchBarContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
                        <FinalSearchBar
                            isLoading={isUserInitializing || !appConfig.loaded}
                            id={V2_SEARCH_BAR_ID}
                            style={styles.searchBoxContainer}
                            autoCompleteStyle={styles.searchBox}
                            inputStyle={styles.input}
                            initialQuery={initialQuery}
                            placeholderText={placeholderText}
                            suggestions={suggestions}
                            onSearch={onSearch}
                            onQueryChange={onQueryChange}
                            entityRegistry={entityRegistry}
                            setIsSearchBarFocused={setIsSearchBarFocused}
                            viewsEnabled={viewsEnabled}
                            isShowNavBarRedesign={isShowNavBarRedesign}
                            combineSiblings
                            fixAutoComplete
                            showQuickFilters
                            showViewAllResults
                            showCommandK
                        />
                        {isShowNavBarRedesign && (
                            <StyledButton type="link" onClick={searchViewAll}>
                                View all <ArrowRight />
                            </StyledButton>
                        )}
                    </SearchBarContainer>
                </Header>
            </Wrapper>
        </>
    );
};
