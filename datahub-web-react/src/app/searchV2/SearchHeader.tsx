import React, { useState } from 'react';
import { Layout } from 'antd';
import styled from 'styled-components';
import { V2_SEARCH_BAR_ID } from '../onboarding/configV2/HomePageOnboardingConfig';
import { SearchBar } from './SearchBar';
import { ManageAccount } from '../shared/ManageAccount';
import { AutoCompleteResultForEntity, EntityType } from '../../types.generated';
import { EntityRegistry } from '../../entityRegistryContext';
import { useAppConfig } from '../useAppConfig';

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
        flex: 1,
        justifyContent: 'center',
    },
};

const SearchBarContainer = styled.div`
    display: flex;
    flex: 1;
    align-items: center;
    justify-content: center;
    margin-left: 80px;
`;

const NavGroup = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-end;
`;

type Props = {
    initialQuery: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string, type?: EntityType) => void;
    onQueryChange: (query: string) => void;
    authenticatedUserUrn: string;
    authenticatedUserPictureLink?: string | null;
    entityRegistry: EntityRegistry;
};

const defaultProps = {
    authenticatedUserPictureLink: undefined,
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
    authenticatedUserUrn,
    authenticatedUserPictureLink,
    entityRegistry,
}: Props) => {
    const [, setIsSearchBarFocused] = useState(false);
    const appConfig = useAppConfig();
    const viewsEnabled = appConfig.config?.viewsConfig?.enabled || false;

    return (
        <div style={styles.container as any}>
            <Header style={styles.header as any}>
                <SearchBarContainer id={V2_SEARCH_BAR_ID}>
                    <SearchBar
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
                <NavGroup>
                    <ManageAccount urn={authenticatedUserUrn} pictureLink={authenticatedUserPictureLink || ''} />
                </NavGroup>
            </Header>
        </div>
    );
};

SearchHeader.defaultProps = defaultProps;
