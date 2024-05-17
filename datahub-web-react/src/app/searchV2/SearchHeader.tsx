import React, { useContext, useState } from 'react';
import { Layout } from 'antd';
import styled from 'styled-components';
import { V2_SEARCH_BAR_ID } from '../onboarding/configV2/HomePageOnboardingConfig';
import { SearchBar } from './SearchBar';
import { AutoCompleteResultForEntity, EntityType } from '../../types.generated';
import { EntityRegistry } from '../../entityRegistryContext';
import { useAppConfig } from '../useAppConfig';
import OnboardingContext from '../onboarding/OnboardingContext';

const { Header } = Layout;

const styles = {
    container: {
        height: 100,
        position: 'fixed',
        zIndex: 10,
        width: '100%',
        lineHeight: '20px',
        padding: '0px 12px',
        backgroundColor: '#171723',
    },
    header: {
        height: 60,
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        backgroundColor: '#171723',
    },
    input: {
        backgroundColor: '#343444',
    },
    searchBox: {
        maxWidth: 620,
        minWidth: 400,
    },
    searchBoxContainer: {
        padding: 0,
        display: 'flex',
        justifyContent: 'center',
        width: '620px',
        minWidth: '400px',
    },
};

const SearchBarContainer = styled.div`
    display: flex;
    flex: 1;
    align-items: center;
    justify-content: center;
    margin-left: 80px;
    margin-top: 6px;
`;

type Props = {
    initialQuery: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string, type?: EntityType) => void;
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

    return (
        <div style={styles.container as any}>
            <Header style={styles.header as any}>
                <SearchBarContainer>
                    <SearchBar
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
                        combineSiblings
                        fixAutoComplete
                        showQuickFilters
                        showViewAllResults
                        showCommandK
                    />
                </SearchBarContainer>
            </Header>
        </div>
    );
};
