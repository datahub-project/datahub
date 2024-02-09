import React, { useState } from 'react';
import { Layout } from 'antd';
import styled from 'styled-components';

import { SearchBar } from './SearchBar';
import { ManageAccount } from '../shared/ManageAccount';
import { AutoCompleteResultForEntity, EntityType } from '../../types.generated';
import EntityRegistry from '../entity/EntityRegistry';
import { ANTD_GRAY } from '../entity/shared/constants';
import { HeaderLinks } from '../shared/admin/HeaderLinks';
import { useAppConfig, useIsShowAcrylInfoEnabled } from '../useAppConfig';
import DemoButton from '../entity/shared/components/styled/DemoButton';
import AppLogoLink from '../shared/AppLogoLink';

const { Header } = Layout;

const styles = {
    header: {
        position: 'fixed',
        zIndex: 10,
        width: '100%',
        lineHeight: '20px',
        padding: '0px 20px',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        borderBottom: `1px solid ${ANTD_GRAY[4.5]}`,
    },
};

const LogoSearchContainer = styled.div`
    display: flex;
    flex: 1;
`;

const NavGroup = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-end;
    min-width: 200px;
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
    const [isSearchBarFocused, setIsSearchBarFocused] = useState(false);
    const showAcrylInfo = useIsShowAcrylInfoEnabled();
    const appConfig = useAppConfig();
    const viewsEnabled = appConfig.config?.viewsConfig?.enabled || false;

    return (
        <Header style={styles.header as any}>
            <LogoSearchContainer>
                <AppLogoLink />
                <SearchBar
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
            </LogoSearchContainer>
            <NavGroup>
                <HeaderLinks areLinksHidden={isSearchBarFocused} />
                <ManageAccount urn={authenticatedUserUrn} pictureLink={authenticatedUserPictureLink || ''} />
                {showAcrylInfo && <DemoButton />}
            </NavGroup>
        </Header>
    );
};

SearchHeader.defaultProps = defaultProps;
