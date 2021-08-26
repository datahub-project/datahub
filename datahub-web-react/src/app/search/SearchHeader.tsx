import * as React from 'react';
import { Image, Layout, Space, Typography } from 'antd';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import { SearchBar } from './SearchBar';
import { ManageAccount } from '../shared/ManageAccount';
import AnalyticsLink from './AnalyticsLink';
import { AutoCompleteResultForEntity, EntityType } from '../../types.generated';
import EntityRegistry from '../entity/EntityRegistry';

const HeaderTitle = styled(Typography.Title)`
    && {
        color: ${(props) => props.theme.styles['layout-header-color']};
        padding-left: 12px;
        margin: 0;
    }
`;

const { Header } = Layout;

const styles = {
    header: {
        position: 'fixed',
        zIndex: 1,
        width: '100%',
        height: '80px',
        lineHeight: '20px',
        padding: '0px 40px',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
    },
    logoImage: { height: '32px', width: 'auto' },
};

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

const NavGroup = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    min-width: 200px;
`;

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
    const themeConfig = useTheme();

    return (
        <Header style={styles.header as any}>
            <Link to="/">
                <Space size={4}>
                    <Image style={styles.logoImage} src={themeConfig.assets.logoUrl} preview={false} />
                    <HeaderTitle level={4}>{themeConfig.content.title}</HeaderTitle>
                </Space>
            </Link>
            <SearchBar
                initialQuery={initialQuery}
                placeholderText={placeholderText}
                suggestions={suggestions}
                onSearch={onSearch}
                onQueryChange={onQueryChange}
                entityRegistry={entityRegistry}
            />
            <NavGroup>
                <AnalyticsLink />
                <ManageAccount urn={authenticatedUserUrn} pictureLink={authenticatedUserPictureLink || ''} />
            </NavGroup>
        </Header>
    );
};

SearchHeader.defaultProps = defaultProps;
